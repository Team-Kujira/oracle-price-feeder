package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	bitgetWSHost        = "ws.bitget.com"
	bitgetWSPath        = "/spot/v1/stream"
	bitgetReconnectTime = time.Minute * 2
	bitgetRestHost      = "https://api.bitget.com"
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

	// BitgetWsSubscriptionMsg Msg to subscribe all at once.
	BitgetWsSubscriptionMsg struct {
		Operation string        `json:"op"`   // Operation (e.x. "subscribe")
		Args      []BitgetWsArg `json:"args"` // Arguments to subscribe to
	}

	BitgetWsArg struct {
		InstType string `json:"instType"` // Instrument type (e.g. "sp")
		Channel  string `json:"channel"`  // Channel (e.x. "ticker" / "candle5m")
		InstID   string `json:"instId"`   // Instrument ID (e.x. BTCUSDT)
	}

	// BitgetWsSubscriptionResponse is the structure for bitget subscription errors.
	BitgetWsSubscriptionResponse struct {
		Event string      `json:"event"` // e.x. "subscription"
		Args  BitgetWsArg `json:"arg"`
	}

	BitgetWsErrorMsg struct {
		Event string `json:"event"` // e.x. "error"
		Code  uint64 `json:"code"`  // e.x. 30003 for invalid op
		Msg   string `json:"msg"`   // e.x. "INVALID op"
	}

	BitgetWsCandleMsg struct {
		Action string      `json:"action"`
		Args   BitgetWsArg `json:"arg"`
		Data   [][6]string `json:"data"`
	}

	BitgetTicker struct {
		Price  string
		Volume string
		Time   string
	}

	BitgetRestTickerResponse struct {
		Code string                 `json:"code"`
		Data []BitgetRestTickerData `json:"data"`
	}

	BitgetRestTickerData struct {
		Symbol string `json:"symbol"`
		Volume string `json:"baseVol"`
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

	args := make([]BitgetWsArg, len(cps))

	for i, cp := range cps {
		args[i] = BitgetWsArg{
			InstType: "sp",
			Channel:  "candle5m",
			InstID:   cp.String(),
		}
	}

	subscriptionMsgs[0] = BitgetWsSubscriptionMsg{
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
	go func(p *BitgetProvider) {
		requiredSymbols := make(map[string]bool, len(cps))

		for _, cp := range cps {
			symbol := cp.String()
			requiredSymbols[symbol] = true
		}

		resp, err := http.Get(p.endpoints.Rest + "/api/spot/v1/market/tickers")
		if err != nil {
			return
		}
		defer resp.Body.Close()

		var tickerResp BitgetRestTickerResponse
		err = json.NewDecoder(resp.Body).Decode(&tickerResp)
		if err != nil {
			return
		}

		for _, ticker := range tickerResp.Data {
			if _, ok := requiredSymbols[ticker.Symbol]; ok {
				p.setTickerVolume(ticker.Symbol, ticker.Volume)
			}
		}

		p.logger.Info().Msg("Done updating volumes")

	}(p)

	return getTickerPrices(p, cps)
}

func (p *BitgetProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	ticker, ok := p.tickers[cp.String()]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf("bitget failed to get ticker price for %s", cp.String())
	}

	timestamp, err := strconv.ParseInt(ticker.Time, 0, 64)
	if err != nil {
		return types.TickerPrice{}, fmt.Errorf("bitget convert time for %s", cp.String())
	}

	return types.TickerPrice{
		Price:  sdk.MustNewDecFromStr(ticker.Price),
		Volume: sdk.MustNewDecFromStr(ticker.Volume),
		Time:   timestamp,
	}, nil
}

// messageReceived handles the received data from the Bitget websocket.
func (p *BitgetProvider) messageReceived(messageType int, bz []byte) {
	var (
		candleMsg        BitgetWsCandleMsg
		candleErr        error
		subscriptionResp BitgetWsSubscriptionResponse
		errorMsg         BitgetWsErrorMsg
	)

	candleErr = json.Unmarshal(bz, &candleMsg)
	if candleErr == nil && len(candleMsg.Data) > 0 {
		p.setTickerPrice(candleMsg)
		telemetryWebsocketMessage(ProviderBitget, MessageTypeCandle)
		return
	}

	err := json.Unmarshal(bz, &subscriptionResp)
	if err == nil && subscriptionResp.Event == "subscribe" {
		return
	}

	err = json.Unmarshal(bz, &errorMsg)
	if err == nil && errorMsg.Code != 0 {
		p.logger.Error().
			Int("length", len(bz)).
			Str("msg", errorMsg.Msg).
			Str("body", string(bz)).
			Msg("Error on receive bitget message")
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("candle", candleErr).
		Str("msg", string(bz)).
		Msg("Error on receive message")
}

// setTickerPrice updates the price and time value of the ticker provided.
// Sets it with volume = 0, if not present in internal tickers map
func (p *BitgetProvider) setTickerPrice(candleMsg BitgetWsCandleMsg) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	data := candleMsg.Data[len(candleMsg.Data)-1]

	price := data[4]
	timestamp := data[0]

	if ticker, ok := p.tickers[candleMsg.Args.InstID]; ok {
		ticker.Price = price
		ticker.Time = timestamp
		p.tickers[candleMsg.Args.InstID] = ticker
	} else {
		p.tickers[candleMsg.Args.InstID] = BitgetTicker{
			Price:  price,
			Volume: "0",
			Time:   timestamp,
		}
	}
}

func (p *BitgetProvider) setTickerVolume(symbol string, volume string) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if ticker, ok := p.tickers[symbol]; ok {
		ticker.Volume = volume
		p.tickers[symbol] = ticker
	}
}

// GetAvailablePairs returns all pairs to which the provider can subscribe.
func (p *BitgetProvider) GetAvailablePairs() (map[string]struct{}, error) {
	// not used yet, so skipping this unless needed
	return make(map[string]struct{}, 0), nil
}
