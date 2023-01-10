package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"price-feeder/oracle/types"
	"strings"
	"sync"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	phemexWSHost   = "phemex.com"
	phemexWSPath   = "/ws"
	phemexRestHost = "api.phemex.com"
)

var _ Provider = (*PhemexProvider)(nil)

type (
	PhemexProvider struct {
		wsc             *WebsocketController
		logger          zerolog.Logger
		mtx             sync.Mutex
		endpoints       Endpoint
		tickers         map[string]PhemexTickerMsg
		subscribedPairs map[string]types.CurrencyPair
	}

	PhemexSubscriptionMsg struct {
		ID     uint     `json:"id"`
		Method string   `json:"method"`
		Params []string `json:"params"`
	}

	PhemexTickerMsg struct {
		Data      PhemexTicker `json:"spot_market24h"`
		Timestamp uint64       `json:"timestamp"`
	}

	PhemexTicker struct {
		Symbol string `json:"symbol"`
		Price  uint64 `json:"lastEp"`
		Volume uint64 `json:"volumeEv"`
	}
)

func NewPhemexProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*PhemexProvider, error) {
	if endpoints.Name != ProviderPhemex {
		endpoints = Endpoint{
			Name:      ProviderPhemex,
			Rest:      phemexRestHost,
			Websocket: phemexWSHost,
		}
	}

	wsURL := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   phemexWSPath,
	}

	phemexLogger := logger.With().Str("provider", string(ProviderPhemex)).Logger()

	provider := &PhemexProvider{
		logger:          phemexLogger,
		endpoints:       endpoints,
		tickers:         map[string]PhemexTickerMsg{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	setSubscribedPairs(provider, pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderPhemex,
		wsURL,
		provider.GetSubscriptionMsgs(pairs...),
		provider.messageReceived,
		defaultPingDuration,
		websocket.TextMessage,
		phemexLogger,
	)

	go provider.wsc.Start()

	return provider, nil
}

func (p *PhemexProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, len(cps))

	subscriptionMsgs[0] = PhemexSubscriptionMsg{
		ID:     1,
		Method: "spot_market24h.subscribe",
		Params: []string{},
	}

	return subscriptionMsgs
}

func (p *PhemexProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	cp, ok := p.subscribedPairs[s]
	return cp, ok
}

func (p *PhemexProvider) SetSubscribedPair(cp types.CurrencyPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.subscribedPairs[cp.String()] = cp
}

func (p *PhemexProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	return subscribeCurrencyPairs(p, cps)
}

func (p *PhemexProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return p.wsc.AddSubscriptionMsgs(msgs)
}

func (p *PhemexProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	return getTickerPrices(p, cps)
}

func (p *PhemexProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	key := "s" + cp.String()

	ticker, ok := p.tickers[key]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf("phemex failed to get ticker price for %s", key)
	}

	price, err := sdk.NewDecFromStr(fmt.Sprintf("%d", ticker.Data.Price))
	if err != nil {
		return types.TickerPrice{}, fmt.Errorf("phemex failed to convert ticker price for %s", key)
	}

	price = price.QuoInt64(100000000)

	volume, err := sdk.NewDecFromStr(fmt.Sprintf("%d", ticker.Data.Volume))
	if err != nil {
		return types.TickerPrice{}, fmt.Errorf("phemex failed to convert ticker price for %s", key)
	}

	volume = volume.QuoInt64(100000000)

	return types.NewTickerPrice(
		string(ProviderPhemex),
		cp.String(),
		price.String(),
		volume.String(),
		int64(ticker.Timestamp/1000),
	)
}

func (p *PhemexProvider) messageReceived(messageType int, bz []byte) {
	var (
		tickerMsg PhemexTickerMsg
		tickerErr error
	)

	tickerErr = json.Unmarshal(bz, &tickerMsg)
	// symbol for spot ticker has a prefix "s", e.x.: "sBTCUSDT"
	if tickerErr == nil && strings.HasPrefix(tickerMsg.Data.Symbol, "s") {
		if _, ok := p.subscribedPairs[tickerMsg.Data.Symbol[1:]]; ok {
			p.setTickerPair(tickerMsg)
			telemetryWebsocketMessage(ProviderPhemex, MessageTypeTicker)

		}
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("ticker", tickerErr).
		Msg("Error on receive message")
}

func (p *PhemexProvider) setTickerPair(ticker PhemexTickerMsg) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.tickers[ticker.Data.Symbol] = ticker
}

func (p *PhemexProvider) GetAvailablePairs() (map[string]struct{}, error) {
	// not used yet, so skipping this unless needed
	return make(map[string]struct{}, 0), nil
}
