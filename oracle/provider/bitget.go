package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"price-feeder/oracle/types"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	bitgetWSHost        = "ws.bitget.com"
	bitgetWSPath        = "/spot/v1/stream"
	bitgetReconnectTime = time.Minute * 2
	bitgetRestHost      = "https://api.bitget.com"
	bitgetRestPath      = "/api/spot/v1/public/products"
	tickerChannel       = "ticker"
	candleChannel       = "candle5m"
	instType            = "SP"
)

var _ Provider = (*BitgetProvider)(nil)

type (
	// BitgetProvider defines an Oracle provider implemented by the Bitget public
	// API.
	//
	// REF: https://bitgetlimited.github.io/apidoc/en/spot/#tickers-channel
	// REF: https://bitgetlimited.github.io/apidoc/en/spot/#candlesticks-channel
	BitgetProvider struct {
		wsc             *WebsocketController
		logger          zerolog.Logger
		mtx             sync.RWMutex
		endpoints       Endpoint
		tickers         map[string]BitgetTicker       // Symbol => BitgetTicker
		subscribedPairs map[string]types.CurrencyPair // Symbol => types.CurrencyPair
	}

	// BitgetSubscriptionMsg Msg to subscribe all at once.
	BitgetSubscriptionMsg struct {
		Operation string                  `json:"op"`   // Operation (e.x. "subscribe")
		Args      []BitgetSubscriptionArg `json:"args"` // Arguments to subscribe to
	}
	BitgetSubscriptionArg struct {
		InstType string `json:"instType"` // Instrument type (e.g. "sp")
		Channel  string `json:"channel"`  // Channel (e.x. "ticker" / "candle5m")
		InstID   string `json:"instId"`   // Instrument ID (e.x. BTCUSDT)
	}

	// BitgetErrResponse is the structure for bitget subscription errors.
	BitgetErrResponse struct {
		Event string `json:"event"` // e.x. "error"
		Code  uint64 `json:"code"`  // e.x. 30003 for invalid op
		Msg   string `json:"msg"`   // e.x. "INVALID op"
	}
	// BitgetSubscriptionResponse is the structure for bitget subscription confirmations.
	BitgetSubscriptionResponse struct {
		Event string                `json:"event"` // e.x. "subscribe"
		Arg   BitgetSubscriptionArg `json:"arg"`   // subscription event argument
	}

	// BitgetTickerResponse is the structure for bitget ticker messages.
	BitgetTicker struct {
		Action string                `json:"action"` // e.x. "snapshot"
		Arg    BitgetSubscriptionArg `json:"arg"`    // subscription event argument
		Data   []BitgetTickerData    `json:"data"`   // ticker data
	}
	BitgetTickerData struct {
		InstID string `json:"instId"`     // e.x. BTCUSD
		Price  string `json:"last"`       // last price e.x. "12.3907"
		Volume string `json:"baseVolume"` // volume in base asset (e.x. "112247.9173")
		Time   int64  `json:"ts"`         // Timestamp
	}

	// BitgetPairsSummary defines the response structure for a Bitget pairs
	// summary.
	BitgetPairsSummary struct {
		RespCode string           `json:"code"`
		Data     []BitgetPairData `json:"data"`
	}
	BitgetPairData struct {
		Base  string `json:"baseCoin"`
		Quote string `json:"quoteCoin"`
	}
)

// NewBitgetProvider returns a new Bitget provider with the WS connection
// and msg handler.
func NewBitgetProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*BitgetProvider, error) {
	if endpoints.Name != ProviderBitget {
		endpoints = Endpoint{
			Name:      ProviderBitget,
			Rest:      bitgetRestHost,
			Websocket: bitgetWSHost,
		}
	}

	wsURL := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   bitgetWSPath,
	}

	bitgetLogger := logger.With().Str("provider", string(ProviderBitget)).Logger()

	provider := &BitgetProvider{
		logger:          bitgetLogger,
		endpoints:       endpoints,
		tickers:         map[string]BitgetTicker{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	setSubscribedPairs(provider, pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderBitget,
		wsURL,
		provider.GetSubscriptionMsgs(pairs...),
		provider.messageReceived,
		defaultPingDuration,
		websocket.TextMessage,
		bitgetLogger,
	)
	go provider.wsc.Start()

	return provider, nil
}

func (p *BitgetProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, 1)

	args := make([]BitgetSubscriptionArg, len(cps))

	for i, cp := range cps {
		args[i] = BitgetSubscriptionArg{
			InstType: "SP",
			Channel:  "ticker",
			InstID:   cp.String(),
		}
	}

	subscriptionMsgs[0] = BitgetSubscriptionMsg{
		Operation: "subscribe",
		Args:      args,
	}

	return subscriptionMsgs
}

func (p *BitgetProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	return subscribeCurrencyPairs(p, cps)
}

func (p *BitgetProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	cp, ok := p.subscribedPairs[s]
	return cp, ok
}

func (p *BitgetProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return p.wsc.AddSubscriptionMsgs(msgs)
}

func (p *BitgetProvider) SetSubscribedPair(cp types.CurrencyPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.subscribedPairs[cp.String()] = cp
}

// GetTickerPrices returns the tickerPrices based on the saved map.
func (p *BitgetProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	return getTickerPrices(p, cps)
}

func (p *BitgetProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	ticker, ok := p.tickers[cp.String()]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf("bitget failed to get ticker price for %s", cp.String())
	}

	return ticker.toTickerPrice()
}

// messageReceived handles the received data from the Bitget websocket.
func (p *BitgetProvider) messageReceived(messageType int, bz []byte) {
	var (
		tickerResp           BitgetTicker
		tickerErr            error
		errResponse          BitgetErrResponse
		subscriptionResponse BitgetSubscriptionResponse
	)

	err := json.Unmarshal(bz, &errResponse)
	if err == nil && errResponse.Code != 0 {
		p.logger.Error().
			Int("length", len(bz)).
			Str("msg", errResponse.Msg).
			Str("body", string(bz)).
			Msg("Error on receive bitget message")
		return
	}

	err = json.Unmarshal(bz, &subscriptionResponse)
	if err == nil && subscriptionResponse.Event == "subscribe" {
		p.logger.Debug().
			Str("InstID", subscriptionResponse.Arg.InstID).
			Str("Channel", subscriptionResponse.Arg.Channel).
			Str("InstType", subscriptionResponse.Arg.InstType).
			Msg("Bitget subscription confirmed")
		return
	}

	tickerErr = json.Unmarshal(bz, &tickerResp)
	if tickerResp.Arg.Channel == tickerChannel {
		p.setTickerPair(tickerResp)
		telemetryWebsocketMessage(ProviderBitget, MessageTypeTicker)
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("ticker", tickerErr).
		Msg("Error on receive message")
}

func (p *BitgetProvider) setTickerPair(ticker BitgetTicker) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.tickers[ticker.Arg.InstID] = ticker
}

// GetAvailablePairs returns all pairs to which the provider can subscribe.
func (p *BitgetProvider) GetAvailablePairs() (map[string]struct{}, error) {
	resp, err := http.Get(p.endpoints.Rest + bitgetRestPath)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pairsSummary BitgetPairsSummary
	if err := json.NewDecoder(resp.Body).Decode(&pairsSummary); err != nil {
		return nil, err
	}
	if pairsSummary.RespCode != "00000" {
		return nil, fmt.Errorf("unable to get bitget available pairs")
	}

	availablePairs := make(map[string]struct{}, len(pairsSummary.Data))
	for _, pair := range pairsSummary.Data {
		cp := types.CurrencyPair{
			Base:  pair.Base,
			Quote: pair.Quote,
		}
		availablePairs[cp.String()] = struct{}{}
	}

	return availablePairs, nil
}

// toTickerPrice converts current BitgetTicker to TickerPrice.
func (ticker BitgetTicker) toTickerPrice() (types.TickerPrice, error) {
	if len(ticker.Data) < 1 {
		return types.TickerPrice{}, fmt.Errorf("ticker has no data")
	}
	return types.NewTickerPrice(
		string(ProviderBitget),
		ticker.Arg.InstID,
		ticker.Data[0].Price,
		ticker.Data[0].Volume,
		ticker.Data[0].Time,
	)
}