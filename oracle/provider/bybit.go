package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"price-feeder/oracle/types"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	bybitWSHost   = "stream.bybit.com"
	bybitWSPath   = "/spot/public/v3"
	bybitRestHost = "https://api.bybit.com"
	bybitRestPath = "/spot/v3/public/symbols"
)

var _ Provider = (*BybitProvider)(nil)

type (
	BybitProvider struct {
		wsc             *WebsocketController
		logger          zerolog.Logger
		mtx             sync.RWMutex
		endpoints       Endpoint
		tickers         map[string]BybitTicker
		subscribedPairs map[string]types.CurrencyPair
	}

	BybitSubscriptionMsg struct {
		Operation string   `json:"op"`   // Operation (e.x. "subscribe")
		Args      []string `json:"args"` // Arguments to subscribe to
	}

	BybitSubscriptionResponse struct {
		Operation string `json:"op"`
		Success   bool   `json:"success"`
		Message   string `json:"ret_msg"`
	}

	BybitTicker struct {
		Topic string          `json:"topic"` // e.x. "tickers.BTCUSDT"
		Data  BybitTickerData `json:"data"`
	}

	BybitTickerData struct {
		Price  string `json:"c"` // e.x. "12.3907"
		Volume string `json:"v"` // e.x. "112247.9173"
		Symbol string `json:"s"` // e.x. "BTCUSDT"
		Time   int64  `json:"t"` // e.x. 1672698023313
	}

	BybitPairsSummary struct {
		RetCode uint   `json:"retCode"`
		RetMsg  string `json:"retMsg"`
		Result  BybitPairsResult
	}

	BybitPairsResult struct {
		List []BybitPairsData
	}

	BybitPairsData struct {
		Base  string `json:"baseCoin"`
		Quote string `json:"quoteCoin"`
	}
)

// NewBybitProvider returns a new Bybit provider with the WS connection
// and msg handler.
func NewBybitProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*BybitProvider, error) {
	if endpoints.Name != ProviderBybit {
		endpoints = Endpoint{
			Name:      ProviderBybit,
			Rest:      bybitRestHost,
			Websocket: bybitWSHost,
		}
	}

	wsURL := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   bybitWSPath,
	}

	bybitLogger := logger.With().Str("provider", string(ProviderBybit)).Logger()

	provider := &BybitProvider{
		logger:          bybitLogger,
		endpoints:       endpoints,
		tickers:         map[string]BybitTicker{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	provider.setSubscribedPairs(pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderBybit,
		wsURL,
		provider.GetSubscriptionMsgs(pairs...),
		provider.messageReceived,
		defaultPingDuration,
		websocket.TextMessage,
		bybitLogger,
	)
	go provider.wsc.Start()

	return provider, nil
}

func (p *BybitProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, 1)

	args := make([]string, len(cps))

	for i, cp := range cps {
		args[i] = "tickers." + cp.String()
	}

	subscriptionMsgs[0] = BybitSubscriptionMsg{
		Operation: "subscribe",
		Args:      args,
	}

	return subscriptionMsgs
}

// GetTickerPrices returns the tickerPrices based on the saved map.
func (p *BybitProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	return getTickerPrices(p, cps)
}

// messageReceived handles the received data from the Bybit websocket.
func (p *BybitProvider) messageReceived(messageType int, bz []byte) {
	var (
		tickerResp           BybitTicker
		tickerErr            error
		subscriptionResponse BybitSubscriptionResponse
	)

	err := json.Unmarshal(bz, &subscriptionResponse)
	if err == nil && subscriptionResponse.Operation == "subscribe" {
		if subscriptionResponse.Success {
			p.logger.Debug().
				Str("provider", "bybit").
				Msg("Bybit subscription confirmed")
			return
		} else {
			p.logger.Error().
				Str("provider", "bybit").
				Msg(subscriptionResponse.Message)
		}
	}

	tickerErr = json.Unmarshal(bz, &tickerResp)
	if tickerErr == nil && tickerResp.Topic != "" {
		p.setTickerPair(tickerResp)
		telemetryWebsocketMessage(ProviderBybit, MessageTypeTicker)
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("ticker", tickerErr).
		Msg("Error on receive message")
}

func (p *BybitProvider) setTickerPair(ticker BybitTicker) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.tickers[ticker.Topic] = ticker
}

func (p *BybitProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	ticker, ok := p.tickers["tickers."+cp.String()]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf("bybit failed to get ticker price for %s", cp.String())
	}

	return ticker.toTickerPrice()
}

// setSubscribedPairs sets N currency pairs to the map of subscribed pairs.
func (p *BybitProvider) setSubscribedPairs(cps ...types.CurrencyPair) {
	for _, cp := range cps {
		p.subscribedPairs[cp.String()] = cp
	}
}

// GetAvailablePairs returns all pairs to which the provider can subscribe.
func (p *BybitProvider) GetAvailablePairs() (map[string]struct{}, error) {
	resp, err := http.Get(p.endpoints.Rest + bybitRestPath)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pairsSummary BybitPairsSummary
	if err := json.NewDecoder(resp.Body).Decode(&pairsSummary); err != nil {
		return nil, err
	}
	if pairsSummary.RetCode != 0 {
		return nil, fmt.Errorf("unable to get bybit available pairs")
	}

	availablePairs := make(map[string]struct{}, len(pairsSummary.Result.List))
	for _, pair := range pairsSummary.Result.List {
		cp := types.CurrencyPair{
			Base:  pair.Base,
			Quote: pair.Quote,
		}
		availablePairs[cp.String()] = struct{}{}
	}

	return availablePairs, nil
}

// toTickerPrice converts current BybitTicker to TickerPrice.
func (ticker BybitTicker) toTickerPrice() (types.TickerPrice, error) {
	return types.NewTickerPrice(
		string(ProviderBybit),
		ticker.Data.Symbol,
		ticker.Data.Price,
		ticker.Data.Volume,
		ticker.Data.Time,
	)
}

func (p *BybitProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return subscribeCurrencyPairs(p, cps)
}

func (p *BybitProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	cp, ok := p.subscribedPairs[s]
	return cp, ok
}

func (p *BybitProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return p.wsc.AddSubscriptionMsgs(msgs)
}

func (p *BybitProvider) SetSubscribedPair(cp types.CurrencyPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.subscribedPairs[cp.String()] = cp
}
