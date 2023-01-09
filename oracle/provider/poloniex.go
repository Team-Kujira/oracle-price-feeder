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
	poloniexWSHost   = "ws.poloniex.com"
	poloniexWSPath   = "/ws/public"
	poloniexRestHost = "api.poloniex.com"
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

	PoloniexSubscriptionMsg struct {
		Event   string   `json:"event"`
		Channel []string `json:"channel"`
		Symbols []string `json:"symbols"`
	}

	PoloniexTickerMsg struct {
		Channel string           `json:"channel"`
		Data    []PoloniexTicker `json:"data"`
	}

	PoloniexTicker struct {
		Symbol string `json:"symbol"`
		Price  string `json:"close"`
		Volume string `json:"quantity"`
		Time   int64  `json:"closeTime"`
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

	go provider.wsc.Start()

	return provider, nil
}

func (p *PoloniexProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, len(cps))

	symbols := make([]string, len(cps))

	for i, cp := range cps {
		symbols[i] = cp.Join("_")
	}

	subscriptionMsgs[0] = PoloniexSubscriptionMsg{
		Event:   "subscribe",
		Channel: []string{"ticker"},
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
		tickerMsg PoloniexTickerMsg
		tickerErr error
	)

	tickerErr = json.Unmarshal(bz, &tickerMsg)
	if tickerErr == nil && len(tickerMsg.Data) == 1 {
		p.setTickerPair(tickerMsg)
		telemetryWebsocketMessage(ProviderPoloniex, MessageTypeTicker)
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("ticker", tickerErr).
		Msg("Error on receive message")
}

func (p *PoloniexProvider) setTickerPair(ticker PoloniexTickerMsg) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	for _, ticker := range ticker.Data {
		p.tickers[ticker.Symbol] = ticker
	}
}

func (p *PoloniexProvider) GetAvailablePairs() (map[string]struct{}, error) {
	// not used yet, so skipping this unless needed
	return make(map[string]struct{}, 0), nil
}
