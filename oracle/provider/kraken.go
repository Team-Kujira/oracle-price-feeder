package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"sync"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	krakenWSHost   = "ws.kraken.com"
	KrakenRestHost = "https://api.kraken.com"
	KrakenRestPath = "/0/public/AssetPairs"
)

var _ Provider = (*KrakenProvider)(nil)

type (
	// KrakenProvider defines an Oracle provider implemented by the Kraken public
	// API.
	//
	// REF: https://docs.kraken.com/websockets/#overview
	KrakenProvider struct {
		wsc             *WebsocketController
		logger          zerolog.Logger
		mtx             sync.RWMutex
		endpoints       Endpoint
		tickers         map[string]KrakenTicker       // Symbol => TickerPrice
		subscribedPairs map[string]types.CurrencyPair // Symbol => types.CurrencyPair
		symbolMapping   map[string]string
	}

	KrakenWsSubscriptionMsg struct {
		Event        string                      `json:"event"`        // subscribe/unsubscribe
		Pair         []string                    `json:"pair"`         // Array of currency pairs ex.: "BTC/USDT",
		Subscription KrakenWsSubscriptionChannel `json:"subscription"` // subscription object
	}

	KrakenWsSubscriptionChannel struct {
		Name string `json:"name"` // channel to be subscribed ex.: ticker
	}

	KrakenWsStatusMsg struct {
		Event  string `json:"event"` // events from kraken ex.: systemStatus | subscriptionStatus
		Status string `json:"status"`
	}

	KrakenRestTickerResponse struct {
		Result map[string]KrakenRestTicker `json:"result"`
	}

	KrakenRestTicker struct {
		Volumes []string `json:"v"`
	}

	KrakenTicker struct {
		Price  string
		Volume string
		Time   string
	}
)

// NewKrakenProvider returns a new Kraken provider with the WS connection and msg handler.
func NewKrakenProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*KrakenProvider, error) {
	if endpoints.Name != ProviderKraken {
		endpoints = Endpoint{
			Name:      ProviderKraken,
			Rest:      KrakenRestHost,
			Websocket: krakenWSHost,
		}
	}

	wsURL := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
	}

	krakenLogger := logger.With().Str("provider", string(ProviderKraken)).Logger()

	provider := &KrakenProvider{
		logger:          krakenLogger,
		endpoints:       endpoints,
		tickers:         map[string]KrakenTicker{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	provider.symbolMapping = map[string]string{
		"BTC":  "XBT",
		"LUNA": "LUNA2",
	}

	setSubscribedPairs(provider, pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderKraken,
		wsURL,
		provider.GetSubscriptionMsgs(pairs...),
		provider.messageReceived,
		disabledPingDuration,
		websocket.PingMessage,
		krakenLogger,
	)

	go provider.wsc.Start()

	return provider, nil
}

func (p *KrakenProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, 1)

	pairs := make([]string, len(cps))

	for i, cp := range cps {
		pairs[i] = p.currencyPairToKrakenSymbol(cp)
	}

	subscriptionMsgs[0] = KrakenWsSubscriptionMsg{
		Event: "subscribe",
		Pair:  pairs,
		Subscription: KrakenWsSubscriptionChannel{
			Name: "ohlc",
		},
	}

	return subscriptionMsgs
}

func (p *KrakenProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	cp, ok := p.subscribedPairs[s]
	return cp, ok
}

func (p *KrakenProvider) SetSubscribedPair(cp types.CurrencyPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.subscribedPairs[cp.String()] = cp
}

func (p *KrakenProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	return subscribeCurrencyPairs(p, cps)
}

func (p *KrakenProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return p.wsc.AddSubscriptionMsgs(msgs)
}

// GetTickerPrices returns the tickerPrices based on the saved map.
func (p *KrakenProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	go func(p *KrakenProvider) {
		requiredSymbols := make(map[string]string, len(cps))

		for _, cp := range cps {
			restSymbol := p.currencyPairToKrakenRestSymbol(cp)
			krakenSymbol := p.currencyPairToKrakenSymbol(cp)
			requiredSymbols[restSymbol] = krakenSymbol
		}

		resp, err := http.Get(p.endpoints.Rest + "/0/public/Ticker")
		if err != nil {
			return
		}
		defer resp.Body.Close()

		var tickerResp KrakenRestTickerResponse
		err = json.NewDecoder(resp.Body).Decode(&tickerResp)
		if err != nil {
			return
		}

		for krakenSymbol, ticker := range tickerResp.Result {
			if symbol, ok := requiredSymbols[krakenSymbol]; ok {
				p.setTickerVolume(symbol, ticker.Volumes[1])
			}
		}

		p.logger.Info().Msg("Done updating volumes")

	}(p)

	return getTickerPrices(p, cps)
}

func (p *KrakenProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	key := p.currencyPairToKrakenSymbol(cp)

	ticker, ok := p.tickers[key]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf("kraken failed to get ticker price for %s", key)
	}

	timestamp, err := strconv.ParseFloat(ticker.Time, 64)
	if err != nil {
		return types.TickerPrice{}, fmt.Errorf("kraken failed parse timestamp for %s", key)
	}

	return types.TickerPrice{
		Price:  sdk.MustNewDecFromStr(ticker.Price),
		Volume: sdk.MustNewDecFromStr(ticker.Volume),
		Time:   int64(math.Floor(timestamp * 1000)),
	}, nil
}

// messageReceived handles any message sent by the provider.
func (p *KrakenProvider) messageReceived(messageType int, bz []byte) {
	if messageType != websocket.TextMessage {
		return
	}

	var (
		statusMsg  KrakenWsStatusMsg
		statusErr  error
		candleMsg  [4]json.RawMessage
		channel    string
		channelErr error
	)

	err := json.Unmarshal(bz, &candleMsg)
	if err == nil {
		channelErr = json.Unmarshal(candleMsg[2], &channel)
		if channelErr == nil && channel == "ohlc-1" {
			p.setTickerPrice(candleMsg)
			return
		}
	}

	statusErr = json.Unmarshal(bz, &statusMsg)
	if statusErr == nil {
		switch statusMsg.Event {
		case "systemStatus":
			if statusMsg.Status == "online" {
				return
			}
		case "subscriptionStatus":
			if statusMsg.Status != "error" {
				return
			}
		case "heartbeat":
			return
		}
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("status", statusErr).
		Str("msg", string(bz)).
		Msg("Error on receive message")
}

func (p *KrakenProvider) setTickerPrice(candleMsg [4]json.RawMessage) {
	var (
		candleData [9]json.RawMessage
		price      string
		symbol     string
		timestamp  string
	)

	err := json.Unmarshal(candleMsg[3], &symbol)
	if err != nil {
		p.logger.Error().
			Str("object", string(candleMsg[3])).
			Msg("failed to unmarshal symbol")
		return
	}

	err = json.Unmarshal(candleMsg[1], &candleData)
	if err != nil {
		p.logger.Error().
			Str("object", string(candleMsg[1])).
			Msg("failed to unmarshal candle")
		return
	}

	err = json.Unmarshal(candleData[0], &timestamp)
	if err != nil {
		p.logger.Error().
			Str("object", string(candleData[0])).
			Msg("failed to unmarshal timestamp")
		return
	}

	err = json.Unmarshal(candleData[5], &price)
	if err != nil {
		p.logger.Error().
			Str("object", string(candleData[5])).
			Msg("failed to unmarshal price")
		return
	}

	p.logger.Error().
		Str("symbol", symbol).
		Str("price", price)

	p.mtx.Lock()
	defer p.mtx.Unlock()

	if ticker, ok := p.tickers[symbol]; ok {
		ticker.Price = price
		ticker.Time = timestamp
		p.tickers[symbol] = ticker
	} else {
		p.tickers[symbol] = KrakenTicker{
			Price:  price,
			Volume: "0",
			Time:   timestamp,
		}
	}
}

func (p *KrakenProvider) setTickerVolume(symbol string, volume string) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if ticker, ok := p.tickers[symbol]; ok {
		ticker.Volume = volume
		p.tickers[symbol] = ticker
	}
}

func (p *KrakenProvider) GetAvailablePairs() (map[string]struct{}, error) {
	// not used yet, so skipping this unless needed
	return make(map[string]struct{}, 0), nil
}

func (p *KrakenProvider) currencyPairToKrakenSymbol(cp types.CurrencyPair) string {
	base, ok := p.symbolMapping[cp.Base]
	if !ok {
		base = cp.Base
	}
	return base + "/" + cp.Quote
}

func (p *KrakenProvider) currencyPairToKrakenRestSymbol(cp types.CurrencyPair) string {
	base, ok := p.symbolMapping[cp.Base]
	if !ok {
		base = cp.Base
	}

	if base == "XBT" && cp.Quote == "USD" {
		return "XXBTZUSD"
	}

	return base + cp.Quote
}
