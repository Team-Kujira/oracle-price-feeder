package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"price-feeder/oracle/types"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	hitbtcWSHost   = "api.hitbtc.com"
	hitbtcWSPath   = "/api/3/ws/public"
	hitbtcRestHost = "api.hitbtc.com"
)

var _ Provider = (*HitbtcProvider)(nil)

type (
	HitbtcProvider struct {
		wsc             *WebsocketController
		logger          zerolog.Logger
		mtx             sync.Mutex
		endpoints       Endpoint
		tickers         map[string]HitbtcTicker
		subscribedPairs map[string]types.CurrencyPair
	}

	HitbtcSubscriptionMsg struct {
		Channel string                   `json:"ch"`
		Method  string                   `json:"method"`
		Params  HitbtcSubscriptionParams `json:"params"`
	}

	HitbtcSubscriptionParams struct {
		Symbols []string `json:"symbols"`
	}

	HitbtcTicker struct {
		Price  string `json:"c"`
		Volume string `json:"v"`
		Time   int64  `json:"t"`
	}

	HitbtcTickerMsg struct {
		Data map[string]HitbtcTicker `json:"data"`
	}
)

func NewHitbtcProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*HitbtcProvider, error) {
	if endpoints.Name != ProviderHitbtc {
		endpoints = Endpoint{
			Name:      ProviderHitbtc,
			Rest:      hitbtcRestHost,
			Websocket: hitbtcWSHost,
		}
	}

	wsURL := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   hitbtcWSPath,
	}

	hitbtcLogger := logger.With().Str("provider", string(ProviderHitbtc)).Logger()

	provider := &HitbtcProvider{
		logger:          hitbtcLogger,
		endpoints:       endpoints,
		tickers:         map[string]HitbtcTicker{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	setSubscribedPairs(provider, pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderHitbtc,
		wsURL,
		provider.GetSubscriptionMsgs(pairs...),
		provider.messageReceived,
		defaultPingDuration,
		websocket.TextMessage,
		hitbtcLogger,
	)

	go provider.wsc.Start()

	return provider, nil
}

func (p *HitbtcProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, len(cps))

	symbols := make([]string, len(cps))

	for i, cp := range cps {
		symbols[i] = cp.String()
	}

	subscriptionMsgs[0] = HitbtcSubscriptionMsg{
		Channel: "ticker/price/1s",
		Method:  "subscribe",
		Params: HitbtcSubscriptionParams{
			Symbols: symbols,
		},
	}

	return subscriptionMsgs
}

func (p *HitbtcProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	cp, ok := p.subscribedPairs[s]
	return cp, ok
}

func (p *HitbtcProvider) SetSubscribedPair(cp types.CurrencyPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.subscribedPairs[cp.String()] = cp
}

func (p *HitbtcProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	return subscribeCurrencyPairs(p, cps)
}

func (p *HitbtcProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return p.wsc.AddSubscriptionMsgs(msgs)
}

func (p *HitbtcProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	return getTickerPrices(p, cps)
}

func (p *HitbtcProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	key := cp.String()

	ticker, ok := p.tickers[key]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf("hitbtc failed to get ticker price for %s", key)
	}

	return types.NewTickerPrice(
		string(ProviderHitbtc),
		key,
		ticker.Price,
		ticker.Volume,
		ticker.Time,
	)
}

func (p *HitbtcProvider) messageReceived(messageType int, bz []byte) {
	var (
		tickerMsg HitbtcTickerMsg
		tickerErr error
	)

	tickerErr = json.Unmarshal(bz, &tickerMsg)
	if tickerErr == nil && len(tickerMsg.Data) == 1 {
		p.setTickerPair(tickerMsg)
		telemetryWebsocketMessage(ProviderHitbtc, MessageTypeTicker)
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("ticker", tickerErr).
		Msg("Error on receive message")
}

func (p *HitbtcProvider) setTickerPair(ticker HitbtcTickerMsg) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	for symbol := range ticker.Data {
		p.tickers[symbol] = ticker.Data[symbol]
	}
}

func (p *HitbtcProvider) GetAvailablePairs() (map[string]struct{}, error) {
	// not used yet, so skipping this unless needed
	return make(map[string]struct{}, 0), nil
}
