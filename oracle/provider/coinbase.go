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
		tickers         map[string]CoinbaseTicker     // Symbol => CoinbaseTicker
		subscribedPairs map[string]types.CurrencyPair // Symbol => types.CurrencyPair
	}

	// CoinbaseSubscriptionMsg Msg to subscribe to all channels.
	CoinbaseSubscriptionMsg struct {
		Type       string   `json:"type"`        // ex. "subscribe"
		ProductIDs []string `json:"product_ids"` // streams to subscribe ex.: ["BOT-USDT", ...]
		Channels   []string `json:"channels"`    // channels to subscribe to ex.: "ticker"
	}

	// CoinbaseTicker defines the ticker info we'd like to save.
	CoinbaseTicker struct {
		ProductID string `json:"product_id"` // ex.: ATOM-USDT
		Price     string `json:"price"`      // ex.: 523.0
		Volume    string `json:"volume_24h"` // 24-hour volume
		Time      string `json:"time"`       // timestamp
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
		tickers:         map[string]CoinbaseTicker{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	provider.setSubscribedPairs(pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderCoinbase,
		wsURL,
		provider.getSubscriptionMsgs(pairs...),
		provider.messageReceived,
		defaultPingDuration,
		websocket.PingMessage,
		coinbaseLogger,
	)
	go provider.wsc.Start()

	return provider, nil
}

func (p *CoinbaseProvider) getSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, 0, 1)

	productIDs := make([]string, len(cps))
	index := 0

	for _, cp := range cps {
		productIDs[index] = cp.Join("-")
		index++
	}

	msg := CoinbaseSubscriptionMsg{
		Type:       "subscribe",
		ProductIDs: productIDs,
		Channels:   []string{"ticker"},
	}
	subscriptionMsgs = append(subscriptionMsgs, msg)
	return subscriptionMsgs
}

// SubscribeCurrencyPairs sends the new subscription messages to the websocket
// and adds them to the providers subscribedPairs array
func (p *CoinbaseProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	newPairs := []types.CurrencyPair{}
	for _, cp := range cps {
		if _, ok := p.subscribedPairs[cp.String()]; !ok {
			newPairs = append(newPairs, cp)
		}
	}

	newSubscriptionMsgs := p.getSubscriptionMsgs(newPairs...)
	if err := p.wsc.AddSubscriptionMsgs(newSubscriptionMsgs); err != nil {
		return err
	}
	p.setSubscribedPairs(newPairs...)
	return nil
}

// GetTickerPrices returns the tickerPrices based on the saved map.
func (p *CoinbaseProvider) GetTickerPrices(pairs ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	tickerPrices := make(map[string]types.TickerPrice, len(pairs))

	for _, currencyPair := range pairs {
		price, err := p.getTickerPrice(currencyPair)
		if err != nil {
			return nil, err
		}

		tickerPrices[currencyPair.String()] = price
	}

	return tickerPrices, nil
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

func (p *CoinbaseProvider) getTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
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
		tickerResp CoinbaseTicker
		tickerErr  error
	)

	tickerErr = json.Unmarshal(bz, &tickerResp)
	if tickerErr == nil {
		p.setTickerPair(tickerResp)
		telemetryWebsocketMessage(ProviderBinance, MessageTypeTicker)
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("ticker", tickerErr).
		Msg("Error on receive message")
}

func (p *CoinbaseProvider) setTickerPair(ticker CoinbaseTicker) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.tickers[ticker.ProductID] = ticker
}

// setSubscribedPairs sets N currency pairs to the map of subscribed pairs.
func (p *CoinbaseProvider) setSubscribedPairs(cps ...types.CurrencyPair) {
	for _, cp := range cps {
		p.subscribedPairs[cp.String()] = cp
	}
}

func (ticker CoinbaseTicker) toTickerPrice() (types.TickerPrice, error) {
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
