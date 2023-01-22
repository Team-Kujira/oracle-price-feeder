package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"price-feeder/oracle/types"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	poloniexWSHost   = "ws.poloniex.com"
	poloniexWSPath   = "/ws/public"
	poloniexRestHost = "https://api.poloniex.com"
)

var _ Provider = (*PoloniexProvider)(nil)

type (
	PoloniexProvider struct {
		wsc             *WebsocketController
		logger          zerolog.Logger
		mtx             sync.Mutex
		endpoints       Endpoint
		tickers         map[string]PoloniexTicker
		subscribedPairs map[string]types.CurrencyPair
	}

	PoloniexWsSubscriptionMsg struct {
		Event   string   `json:"event"`
		Channel []string `json:"channel"`
		Symbols []string `json:"symbols"`
	}

	PoloniexWsGenericResponse struct {
		Event string `json:"event"`
	}

	PoloniexWsCandleMsg struct {
		Channel string                 `json:"channel"`
		Data    []PoloniexWsCandleData `json:"data"`
	}

	PoloniexWsCandleData struct {
		Symbol string `json:"symbol"`
		Close  string `json:"close"`
		Time   int64  `json:"ts"`
	}

	PoloniexRest24hTicker struct {
		Symbol string `json:"symbol"`
		Volume string `json:"quantity"`
	}

	PoloniexTicker struct {
		Price  string
		Volume string
		Time   int64
	}
)

func NewPoloniexProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*PoloniexProvider, error) {
	if endpoints.Name != ProviderPoloniex {
		endpoints = Endpoint{
			Name:      ProviderPoloniex,
			Rest:      poloniexRestHost,
			Websocket: poloniexWSHost,
		}
	}

	wsURL := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   poloniexWSPath,
	}

	poloniexLogger := logger.With().Str("provider", string(ProviderPoloniex)).Logger()

	provider := &PoloniexProvider{
		logger:          poloniexLogger,
		endpoints:       endpoints,
		tickers:         map[string]PoloniexTicker{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	setSubscribedPairs(provider, pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderPoloniex,
		wsURL,
		provider.GetSubscriptionMsgs(pairs...),
		provider.messageReceived,
		defaultPingDuration,
		websocket.TextMessage,
		poloniexLogger,
	)

	provider.wsc.pingMessage = `{"event":"ping"}`

	go provider.wsc.Start()

	return provider, nil
}

func (p *PoloniexProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, 1)

	symbols := make([]string, len(cps))

	for i, cp := range cps {
		symbols[i] = cp.Join("_")
	}

	subscriptionMsgs[0] = PoloniexWsSubscriptionMsg{
		Event:   "subscribe",
		Channel: []string{"candles_minute_1"},
		Symbols: symbols,
	}

	return subscriptionMsgs
}

func (p *PoloniexProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	cp, ok := p.subscribedPairs[s]
	return cp, ok
}

func (p *PoloniexProvider) SetSubscribedPair(cp types.CurrencyPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.subscribedPairs[cp.String()] = cp
}

func (p *PoloniexProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	return subscribeCurrencyPairs(p, cps)
}

func (p *PoloniexProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return p.wsc.AddSubscriptionMsgs(msgs)
}

func (p *PoloniexProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	go func(p *PoloniexProvider) {
		requiredSymbols := make(map[string]bool, len(cps))

		for _, cp := range cps {
			requiredSymbols[cp.Join("_")] = true
		}

		resp, err := http.Get(p.endpoints.Rest + "/markets/ticker24h")
		if err != nil {
			p.logger.Error().Msg("failed to get tickers")
			return
		}
		defer resp.Body.Close()

		var tickerResp []PoloniexRest24hTicker
		err = json.NewDecoder(resp.Body).Decode(&tickerResp)
		if err != nil {
			p.logger.Error().Msg("failed to parse rest response")
			return
		}

		for _, ticker := range tickerResp {
			if _, ok := requiredSymbols[ticker.Symbol]; ok {
				p.setTickerVolume(ticker.Symbol, ticker.Volume)
			}
		}

		p.logger.Info().Msg("Done updating volumes")
	}(p)

	return getTickerPrices(p, cps)
}

func (p *PoloniexProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	key := cp.Join("_")

	ticker, ok := p.tickers[key]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf("poloniex failed to get ticker price for %s", key)
	}

	return types.NewTickerPrice(
		string(ProviderPoloniex),
		key,
		ticker.Price,
		ticker.Volume,
		ticker.Time,
	)
}

func (p *PoloniexProvider) messageReceived(messageType int, bz []byte) {
	var (
		candleMsg   PoloniexWsCandleMsg
		candleErr   error
		genericResp PoloniexWsGenericResponse
	)

	candleErr = json.Unmarshal(bz, &candleMsg)
	if candleErr == nil && len(candleMsg.Data) == 1 {
		p.setTickerPrice(candleMsg)
		telemetryWebsocketMessage(ProviderPoloniex, MessageTypeCandle)
		return
	}

	err := json.Unmarshal(bz, &genericResp)
	if err == nil {
		switch genericResp.Event {
		case "subscribe":
			return
		case "pong":
			return
		}
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("candle", candleErr).
		Str("msg", string(bz)).
		Msg("Error on receive message")
}

func (p *PoloniexProvider) setTickerPrice(candleMsg PoloniexWsCandleMsg) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	data := candleMsg.Data[len(candleMsg.Data)-1]

	symbol := data.Symbol
	price := data.Close
	timestamp := data.Time

	if ticker, ok := p.tickers[symbol]; ok {
		ticker.Price = price
		ticker.Time = timestamp
		p.tickers[symbol] = ticker
	} else {
		p.tickers[symbol] = PoloniexTicker{
			Price:  price,
			Volume: "0",
			Time:   timestamp,
		}
	}
}

func (p *PoloniexProvider) setTickerVolume(symbol string, volume string) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if ticker, ok := p.tickers[symbol]; ok {
		ticker.Volume = volume
		p.tickers[symbol] = ticker
	}
}

func (p *PoloniexProvider) GetAvailablePairs() (map[string]struct{}, error) {
	// not used yet, so skipping this unless needed
	return make(map[string]struct{}, 0), nil
}
