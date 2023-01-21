package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"

	"price-feeder/oracle/types"
)

const (
	coinbaseWSHost    = "ws-feed.exchange.coinbase.com"
	coinbasePingCheck = time.Second * 28 // should be < 30
	coinbaseRestHost  = "https://api.exchange.coinbase.com"
	coinbaseRestPath  = "/products"
	coinbaseTimeFmt   = "2006-01-02T15:04:05.000000Z"
	unixMinute        = 60000
)

var (
	_                  Provider = (*CoinbaseProvider)(nil)
	CoinbaseTimeFormat          = "2006-01-02T15:04:05.000000Z"
)

type (
	// CoinbaseProvider defines an Oracle provider implemented by the Coinbase public
	// API.
	//
	// REF: https://www.coinbase.io/docs/websocket/index.html
	CoinbaseProvider struct {
		wsc             *WebsocketController
		logger          zerolog.Logger
		reconnectTimer  *time.Ticker
		mtx             sync.RWMutex
		endpoints       Endpoint
		tickers         map[string]CoinbaseWsTickerMsg // Symbol => CoinbaseWsTickerMsg
		subscribedPairs map[string]types.CurrencyPair  // Symbol => types.CurrencyPair
	}

	// CoinbaseWsSubscriptionMsg Msg to subscribe to all channels.
	CoinbaseWsSubscriptionMsg struct {
		Type       string   `json:"type"`        // ex. "subscribe"
		ProductIDs []string `json:"product_ids"` // streams to subscribe ex.: ["BOT-USDT", ...]
		Channels   []string `json:"channels"`    // channels to subscribe to ex.: "ticker"
	}

	CoinbaseWsGenericMsg struct {
		Type string `json:"type"`
	}

	// CoinbaseWsTickerMsg defines the ticker info we'd like to save.
	CoinbaseWsTickerMsg struct {
		Type      string `json:"type"`
		ProductID string `json:"product_id"` // ex.: ATOM-USDT
		Price     string `json:"price"`      // ex.: 523.0
		Volume    string `json:"volume_24h"` // 24-hour volume
		Time      string `json:"time"`       // timestamp
	}

	CoinbaseWsMatchMsg struct {
		Type      string `json:"type"`
		ProductID string `json:"product_id"`
		Price     string `json:"price"`
		Time      string `json:"time"`
	}

	// CoinbaseErrResponse defines the response body for errors.
	CoinbaseErrResponse struct {
		Type   string `json:"type"`   // should be "error"
		Reason string `json:"reason"` // ex.: "tickers" is not a valid channel
	}

	// CoinbasePairSummary defines the response structure for a Coinbase pair summary.
	CoinbasePairSummary struct {
		Base  string `json:"base_currency"`
		Quote string `json:"quote_currency"`
	}
)

// NewCoinbaseProvider creates a new CoinbaseProvider.
func NewCoinbaseProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*CoinbaseProvider, error) {
	if endpoints.Name != ProviderCoinbase {
		endpoints = Endpoint{
			Name:      ProviderCoinbase,
			Rest:      coinbaseRestHost,
			Websocket: coinbaseWSHost,
		}
	}
	wsURL := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
	}

	coinbaseLogger := logger.With().Str("provider", string(ProviderCoinbase)).Logger()

	provider := &CoinbaseProvider{
		logger:          coinbaseLogger,
		reconnectTimer:  time.NewTicker(coinbasePingCheck),
		endpoints:       endpoints,
		tickers:         map[string]CoinbaseWsTickerMsg{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	setSubscribedPairs(provider, pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderCoinbase,
		wsURL,
		provider.GetSubscriptionMsgs(pairs...),
		provider.messageReceived,
		defaultPingDuration,
		websocket.PingMessage,
		coinbaseLogger,
	)
	go provider.wsc.Start()

	return provider, nil
}

func (p *CoinbaseProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, 1)

	productIDs := make([]string, len(cps))

	for i, cp := range cps {
		productIDs[i] = cp.Join("-")
	}

	subscriptionMsgs[0] = CoinbaseWsSubscriptionMsg{
		Type:       "subscribe",
		ProductIDs: productIDs,
		Channels:   []string{"ticker_batch", "matches"},
	}

	return subscriptionMsgs
}

func (p *CoinbaseProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	cp, ok := p.subscribedPairs[s]
	return cp, ok
}

func (p *CoinbaseProvider) SetSubscribedPair(cp types.CurrencyPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.subscribedPairs[cp.String()] = cp
}

func (p *CoinbaseProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	return subscribeCurrencyPairs(p, cps)
}

func (p *CoinbaseProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return p.wsc.AddSubscriptionMsgs(msgs)
}

// GetTickerPrices returns the tickerPrices based on the saved map.
func (p *CoinbaseProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	return getTickerPrices(p, cps)
}

func (p *CoinbaseProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	gp := cp.Join("-")
	if tickerPair, ok := p.tickers[gp]; ok {
		return tickerPair.toTickerPrice()
	}

	return types.TickerPrice{}, fmt.Errorf("coinbase failed to get ticker price for %s", gp)
}

func (p *CoinbaseProvider) messageReceived(messageType int, bz []byte) {
	var (
		tickerMsg  CoinbaseWsTickerMsg
		tickerErr  error
		matchMsg   CoinbaseWsMatchMsg
		matchErr   error
		genericMsg CoinbaseWsGenericMsg
		genericErr error
	)

	matchErr = json.Unmarshal(bz, &matchMsg)
	if matchErr == nil && (matchMsg.Type == "match" || matchMsg.Type == "last_match") {
		p.updateTicker(matchMsg)
		telemetryWebsocketMessage(ProviderBinance, MessageTypeTrade)
		return
	}

	tickerErr = json.Unmarshal(bz, &tickerMsg)
	if tickerErr == nil && tickerMsg.Type == "ticker" {
		p.setTickerPair(tickerMsg)
		telemetryWebsocketMessage(ProviderBinance, MessageTypeTicker)
		return
	}

	genericErr = json.Unmarshal(bz, &genericMsg)
	if genericErr == nil && genericMsg.Type == "subscription" {
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("ticker", tickerErr).
		AnErr("match", matchErr).
		AnErr("generic", genericErr).
		Str("msg", string(bz)).
		Msg("Error on receive message")
}

func (p *CoinbaseProvider) setTickerPair(ticker CoinbaseWsTickerMsg) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.tickers[ticker.ProductID] = ticker
}

func (p *CoinbaseProvider) updateTicker(matchMsg CoinbaseWsMatchMsg) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	ticker, ok := p.tickers[matchMsg.ProductID]
	if ok {
		ticker.Price = matchMsg.Price
		ticker.Time = matchMsg.Time
		p.tickers[matchMsg.ProductID] = ticker
	}

}

// GetAvailablePairs returns all pairs to which the provider can subscribe.
func (p *CoinbaseProvider) GetAvailablePairs() (map[string]struct{}, error) {
	resp, err := http.Get(p.endpoints.Rest + coinbaseRestPath)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pairsSummary []CoinbasePairSummary
	if err := json.NewDecoder(resp.Body).Decode(&pairsSummary); err != nil {
		return nil, err
	}

	availablePairs := make(map[string]struct{}, len(pairsSummary))
	for _, pair := range pairsSummary {
		cp := types.CurrencyPair{
			Base:  strings.ToUpper(pair.Base),
			Quote: strings.ToUpper(pair.Quote),
		}
		availablePairs[cp.String()] = struct{}{}
	}

	return availablePairs, nil
}

func (ticker CoinbaseWsTickerMsg) toTickerPrice() (types.TickerPrice, error) {
	timestamp, err := time.Parse(CoinbaseTimeFormat, ticker.Time)
	if err != nil {
		fmt.Println(err)
	}

	return types.NewTickerPrice(
		string(ProviderCoinbase),
		strings.ReplaceAll(ticker.ProductID, "-", ""), // BTC-USDT -> BTCUSDT
		ticker.Price,
		ticker.Volume,
		timestamp.UnixMilli(),
	)
}
