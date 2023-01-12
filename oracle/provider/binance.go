package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"price-feeder/oracle/types"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	binanceWSHost     = "stream.binance.com:9443"
	binanceUSWSHost   = "stream.binance.us:9443"
	binanceWSPath     = "/ws/umeestream"
	binanceRestHost   = "https://api1.binance.com"
	binanceRestUSHost = "https://api.binance.us"
	binanceRestPath   = "/api/v3/ticker/price"
)

var _ Provider = (*BinanceProvider)(nil)

type (
	// BinanceProvider defines an Oracle provider implemented by the Binance public
	// API.
	//
	// REF: https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-mini-ticker-stream
	// REF: https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-streams
	BinanceProvider struct {
		wsc             *WebsocketController
		logger          zerolog.Logger
		mtx             sync.RWMutex
		endpoints       Endpoint
		tickers         map[string]BinanceTicker      // Symbol => BinanceTicker
		subscribedPairs map[string]types.CurrencyPair // Symbol => types.CurrencyPair
	}

	// BinanceTicker ticker price response. https://pkg.go.dev/encoding/json#Unmarshal
	// Unmarshal matches incoming object keys to the keys used by Marshal (either the
	// struct field name or its tag), preferring an exact match but also accepting a
	// case-insensitive match. C field which is Statistics close time is not used, but
	// it avoids to implement specific UnmarshalJSON.
	BinanceTicker struct {
		Symbol    string `json:"s"` // Symbol ex.: BTCUSDT
		LastPrice string `json:"c"` // Last price ex.: 0.0025
		Volume    string `json:"v"` // Total traded base asset volume ex.: 1000
		Time      uint64 `json:"C"` // Statistics close time
	}

	// BinanceSubscribeMsg Msg to subscribe all the tickers channels.
	BinanceSubscriptionMsg struct {
		Method string   `json:"method"` // SUBSCRIBE/UNSUBSCRIBE
		Params []string `json:"params"` // streams to subscribe ex.: usdtatom@ticker
		ID     uint16   `json:"id"`     // identify messages going back and forth
	}

	// BinanceSubscriptionResp the response structure for a binance subscription response
	BinanceSubscriptionResp struct {
		Result string `json:"result"`
		ID     uint16 `json:"id"`
	}

	// BinancePairSummary defines the response structure for a Binance pair
	// summary.
	BinancePairSummary struct {
		Symbol string `json:"symbol"`
	}
)

func NewBinanceProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	binanceUS bool,
	pairs ...types.CurrencyPair,
) (*BinanceProvider, error) {
	if (endpoints.Name) != ProviderBinance {
		if !binanceUS {
			endpoints = Endpoint{
				Name:      ProviderBinance,
				Rest:      binanceRestHost,
				Websocket: binanceWSHost,
			}
		} else {
			endpoints = Endpoint{
				Name:      ProviderBinanceUS,
				Rest:      binanceRestUSHost,
				Websocket: binanceUSWSHost,
			}
		}
	}

	wsURL := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   binanceWSPath,
	}

	binanceLogger := logger.With().Str("provider", string(ProviderBinance)).Logger()

	provider := &BinanceProvider{
		logger:          binanceLogger,
		endpoints:       endpoints,
		tickers:         map[string]BinanceTicker{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	setSubscribedPairs(provider, pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderBinance,
		wsURL,
		provider.GetSubscriptionMsgs(pairs...),
		provider.messageReceived,
		disabledPingDuration,
		websocket.PingMessage,
		binanceLogger,
	)
	go provider.wsc.Start()

	return provider, nil
}

func (p *BinanceProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	msg := BinanceSubscriptionMsg{
		Method: "SUBSCRIBE",
		Params: make([]string, len(cps)),
		ID:     1,
	}
	for i, cp := range cps {
		msg.Params[i] = strings.ToLower(cp.String()) + "@ticker"
	}
	return []interface{}{msg}
}

func (p *BinanceProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	cp, ok := p.subscribedPairs[s]
	return cp, ok
}

func (p *BinanceProvider) SetSubscribedPair(cp types.CurrencyPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.subscribedPairs[cp.String()] = cp
}

func (p *BinanceProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	return subscribeCurrencyPairs(p, cps)
}

func (p *BinanceProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return p.wsc.AddSubscriptionMsgs(msgs)
}

// GetTickerPrices returns the tickerPrices based on the provided pairs.
func (p *BinanceProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	return getTickerPrices(p, cps)
}

func (p *BinanceProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	key := cp.String()

	ticker, ok := p.tickers[key]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf("binance failed to get ticker price for %s", key)
	}

	return ticker.toTickerPrice()
}

func (p *BinanceProvider) messageReceived(messageType int, bz []byte) {
	var (
		tickerResp       BinanceTicker
		tickerErr        error
		subscribeResp    BinanceSubscriptionResp
		subscribeRespErr error
	)

	tickerErr = json.Unmarshal(bz, &tickerResp)
	if tickerErr == nil {
		p.setTickerPair(tickerResp)
		telemetryWebsocketMessage(ProviderBinance, MessageTypeTicker)
		return
	}

	subscribeRespErr = json.Unmarshal(bz, &subscribeResp)
	if subscribeResp.ID == 1 {
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("ticker", tickerErr).
		AnErr("subscribeResp", subscribeRespErr).
		Str("msg", string(bz)).
		Msg("Error on receive message")
}

func (p *BinanceProvider) setTickerPair(ticker BinanceTicker) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.tickers[ticker.Symbol] = ticker
}

// GetAvailablePairs returns all pairs to which the provider can subscribe.
// ex.: map["ATOMUSDT" => {}, "UMEEUSDC" => {}].
func (p *BinanceProvider) GetAvailablePairs() (map[string]struct{}, error) {
	resp, err := http.Get(p.endpoints.Rest + binanceRestPath)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pairsSummary []BinancePairSummary
	if err := json.NewDecoder(resp.Body).Decode(&pairsSummary); err != nil {
		return nil, err
	}

	availablePairs := make(map[string]struct{}, len(pairsSummary))
	for _, pairName := range pairsSummary {
		availablePairs[strings.ToUpper(pairName.Symbol)] = struct{}{}
	}

	return availablePairs, nil
}

func (ticker BinanceTicker) toTickerPrice() (types.TickerPrice, error) {
	tickerPrice, err := types.NewTickerPrice(
		string(ProviderBinance),
		ticker.Symbol,
		ticker.LastPrice,
		ticker.Volume,
		int64(ticker.Time),
	)
	if err != nil {
		return types.TickerPrice{}, err
	}
	return tickerPrice, nil
}
