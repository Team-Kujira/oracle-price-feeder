package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"price-feeder/oracle/types"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	bitforexWSHost   = "www.bitforex.com"
	bitforexWSPath   = "/mkapi/coinGroup1/ws"
	bitforexRestHost = ""
)

var _ Provider = (*BitforexProvider)(nil)

type (
	BitforexProvider struct {
		wsc             *WebsocketController
		logger          zerolog.Logger
		mtx             sync.Mutex
		endpoints       Endpoint
		tickers         map[string]BitforexTickerData
		subscribedPairs map[string]types.CurrencyPair
	}

	BitforexSubscriptionMsg []BitforexSubscriptionConfig

	BitforexSubscriptionConfig struct {
		Type  string              `json:"type"`
		Event string              `json:"event"`
		Param BitforexTickerParam `json:"param"`
	}

	BitforexTickerMsg struct {
		Event string              `json:"event"`
		Data  BitforexTickerData  `json:"data"`
		Param BitforexTickerParam `json:"param"`
	}

	BitforexTickerData struct {
		Price  float64 `json:"last"`
		Volume float64 `json:"productvol"`
		Time   int64   `json:"enddate"`
	}

	BitforexTickerParam struct {
		BusinessType string `json:"businessType"`
	}
)

func NewBitforexProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*BitforexProvider, error) {
	if endpoints.Name != ProviderBitforex {
		endpoints = Endpoint{
			Name:      ProviderBitforex,
			Rest:      bitforexRestHost,
			Websocket: bitforexWSHost,
		}
	}

	wsURL := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   bitforexWSPath,
	}

	bitforexLogger := logger.With().Str("provider", string(ProviderBitforex)).Logger()

	provider := &BitforexProvider{
		logger:          bitforexLogger,
		endpoints:       endpoints,
		tickers:         map[string]BitforexTickerData{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	setSubscribedPairs(provider, pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderBitforex,
		wsURL,
		provider.GetSubscriptionMsgs(pairs...),
		provider.messageReceived,
		defaultPingDuration,
		websocket.TextMessage,
		bitforexLogger,
	)

	go provider.wsc.Start()

	return provider, nil
}

func (p *BitforexProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, 1)

	subscriptionMsg := make([]BitforexSubscriptionConfig, len(cps))

	for i, cp := range cps {
		businessType := "coin-" + strings.ToLower(cp.Quote+"-"+cp.Base)

		subscriptionMsg[i] = BitforexSubscriptionConfig{
			Type:  "subHq",
			Event: "ticker",
			Param: BitforexTickerParam{
				BusinessType: businessType,
			},
		}
	}

	subscriptionMsgs[0] = subscriptionMsg

	return subscriptionMsgs
}

func (p *BitforexProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	cp, ok := p.subscribedPairs[s]
	return cp, ok
}

func (p *BitforexProvider) SetSubscribedPair(cp types.CurrencyPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.subscribedPairs[cp.String()] = cp
}

func (p *BitforexProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	return subscribeCurrencyPairs(p, cps)
}
func (p *BitforexProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return p.wsc.AddSubscriptionMsgs(msgs)
}

func (p *BitforexProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	return getTickerPrices(p, cps)
}

func (p *BitforexProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	businessType := "coin-" + strings.ToLower(cp.Quote+"-"+cp.Base)

	ticker, ok := p.tickers[businessType]
	if !ok {
		err := fmt.Errorf("bitforex failed to get ticker price for %s", cp)
		return types.TickerPrice{}, err
	}

	return types.NewTickerPrice(
		string(ProviderBitforex),
		cp.String(),
		fmt.Sprintf("%f", ticker.Price),
		fmt.Sprintf("%f", ticker.Volume),
		ticker.Time,
	)
}

func (p *BitforexProvider) messageReceived(messageType int, bz []byte) {
	var (
		tickerMsg BitforexTickerMsg
		tickerErr error
	)

	tickerErr = json.Unmarshal(bz, &tickerMsg)
	if tickerErr == nil && tickerMsg.Event == "ticker" {
		p.setTickerPair(tickerMsg)
		telemetryWebsocketMessage(ProviderBitforex, MessageTypeTicker)
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("ticker", tickerErr).
		Msg(string(bz))
}

func (p *BitforexProvider) setTickerPair(ticker BitforexTickerMsg) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.tickers[ticker.Param.BusinessType] = ticker.Data
}

func (p *BitforexProvider) GetAvailablePairs() (map[string]struct{}, error) {
	// not used yet, so skipping this unless needed
	return make(map[string]struct{}, 0), nil
}
