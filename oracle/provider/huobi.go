package provider

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	huobiWSHost        = "api-aws.huobi.pro"
	huobiWSPath        = "/ws"
	huobiReconnectTime = time.Minute * 2
	huobiRestHost      = "https://api.huobi.pro"
	huobiRestPath      = "/market/tickers"
)

var _ Provider = (*HuobiProvider)(nil)

type (
	// HuobiProvider defines an Oracle provider implemented by the Huobi public
	// API.
	//
	// REF: https://huobiapi.github.io/docs/spot/v1/en/#market-ticker
	// REF: https://huobiapi.github.io/docs/spot/v1/en/#get-klines-candles
	HuobiProvider struct {
		wsc             *WebsocketController
		logger          zerolog.Logger
		mtx             sync.RWMutex
		endpoints       Endpoint
		tickers         map[string]HuobiTicker        // market.$symbol.ticker => HuobiTicker
		subscribedPairs map[string]types.CurrencyPair // Symbol => types.CurrencyPair
	}

	HuobiTicker struct {
		Price  float64
		Volume float64
		Time   int64
	}

	HuobiWsSubscriptionMsg struct {
		Sub string `json:"sub"` // channel to subscribe market.$symbol.ticker
	}

	// b'{"id":null,"status":"ok","subbed":"market.btcusdt.trade.detail","ts":1673949735621}'
	HuobiWsSubscriptionResponse struct {
		Status string `json:"status"`
		Subbed string `json:"subbed"`
	}

	HuobiWsTradeMsg struct {
		Channel string           `json:"ch"`
		Tick    HuobiWsTradeTick `json:"tick"`
	}

	HuobiWsTradeTick struct {
		Data []HuobiWsTradeTickData `json:"data"`
	}

	HuobiWsTradeTickData struct {
		Price float64 `json:"price"`
		Time  int64   `json:"ts"`
	}

	HuobiRestTickerResponse struct {
		Data []HuobiRestTicker `json:"data"`
	}

	HuobiRestTicker struct {
		Symbol string  `json:"symbol"`
		Volume float64 `json:"amount"`
	}

	// HuobiPairsSummary defines the response structure for an Huobi pairs
	// summary.
	HuobiPairsSummary struct {
		Data []HuobiPairData `json:"data"`
	}

	// HuobiPairData defines the data response structure for an Huobi pair.
	HuobiPairData struct {
		Symbol string `json:"symbol"`
	}
)

// NewHuobiProvider returns a new Huobi provider with the WS connection and msg handler.
func NewHuobiProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*HuobiProvider, error) {
	if endpoints.Name != ProviderHuobi {
		endpoints = Endpoint{
			Name:      ProviderHuobi,
			Rest:      huobiRestHost,
			Websocket: huobiWSHost,
		}
	}

	wsURL := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   huobiWSPath,
	}

	huobiLogger := logger.With().Str("provider", string(ProviderHuobi)).Logger()

	provider := &HuobiProvider{
		logger:          huobiLogger,
		endpoints:       endpoints,
		tickers:         map[string]HuobiTicker{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	setSubscribedPairs(provider, pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderHuobi,
		wsURL,
		provider.GetSubscriptionMsgs(pairs...),
		provider.messageReceived,
		disabledPingDuration,
		websocket.PingMessage,
		huobiLogger,
	)
	go provider.wsc.Start()

	return provider, nil
}

func (p *HuobiProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, len(cps))
	for i, cp := range cps {
		subscriptionMsgs[i] = HuobiWsSubscriptionMsg{
			Sub: strings.ToLower("market." + cp.String() + ".trade.detail"),
		}
	}
	return subscriptionMsgs
}

func (p *HuobiProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	cp, ok := p.subscribedPairs[s]
	return cp, ok
}

func (p *HuobiProvider) SetSubscribedPair(cp types.CurrencyPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.subscribedPairs[cp.String()] = cp
}

func (p *HuobiProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	return subscribeCurrencyPairs(p, cps)
}

func (p *HuobiProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return p.wsc.AddSubscriptionMsgs(msgs)
}

// GetTickerPrices returns the tickerPrices based on the saved map.
func (p *HuobiProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {

	go func(p *HuobiProvider) {
		requiredSymbols := make(map[string]string, len(cps))

		for _, cp := range cps {
			symbol := strings.ToLower(cp.String())
			requiredSymbols[symbol] = "market." + symbol + ".trade.detail"
		}

		resp, err := http.Get(p.endpoints.Rest + "/market/tickers")
		if err != nil {
			return
		}
		defer resp.Body.Close()

		var tickerResp HuobiRestTickerResponse
		err = json.NewDecoder(resp.Body).Decode(&tickerResp)
		if err != nil {
			return
		}

		for _, ticker := range tickerResp.Data {
			if symbol, ok := requiredSymbols[ticker.Symbol]; ok {
				p.setTickerVolume(symbol, ticker.Volume)
			}
		}

		p.logger.Info().Msg("Done updating volumes")

	}(p)

	return getTickerPrices(p, cps)
}

func (p *HuobiProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	key := strings.ToLower("market." + cp.String() + ".trade.detail")

	ticker, ok := p.tickers[key]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf("huobi failed to get ticker price for %s", cp.String())
	}

	return types.TickerPrice{
		Price:  sdk.MustNewDecFromStr(fmt.Sprintf("%f", ticker.Price)),
		Volume: sdk.MustNewDecFromStr(fmt.Sprintf("%f", ticker.Volume)),
		Time:   ticker.Time,
	}, nil
}

// messageReceived handles the received data from the Huobi websocket. All return
// data of websocket Market APIs are compressed with GZIP so they need to be
// decompressed.
func (p *HuobiProvider) messageReceived(messageType int, bz []byte) {
	if messageType != websocket.BinaryMessage {
		return
	}

	bz, err := decompressGzip(bz)
	if err != nil {
		p.logger.Err(err).Msg("failed to decompress gziped message")
		return
	}

	if bytes.Contains(bz, ping) {
		p.pong(bz)
		return
	}

	var (
		tradeMsg         HuobiWsTradeMsg
		tradeErr         error
		subscriptionResp HuobiWsSubscriptionResponse
	)

	// sometimes the message received is not a ticker or a candle response.
	tradeErr = json.Unmarshal(bz, &tradeMsg)
	if err == nil && len(tradeMsg.Tick.Data) > 0 {
		p.setTicker(tradeMsg)
		telemetryWebsocketMessage(ProviderHuobi, MessageTypeTrade)
		return
	}

	err = json.Unmarshal(bz, &subscriptionResp)
	if err == nil && subscriptionResp.Status == "ok" {
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("trade", tradeErr).
		Str("msg", string(bz)).
		Msg("Error on receive message")
}

// pong return a heartbeat message when a "ping" is received and reset the
// reconnect ticker because the connection is alive. After connected to Huobi's
// Websocket server, the server will send heartbeat periodically (5s interval).
// When client receives an heartbeat message, it should respond with a matching
// "pong" message which has the same integer in it, e.g. {"ping": 1492420473027}
// and then the return pong message should be {"pong": 1492420473027}.
func (p *HuobiProvider) pong(bz []byte) {
	var heartbeat struct {
		Ping uint64 `json:"ping"`
	}

	if err := json.Unmarshal(bz, &heartbeat); err != nil {
		p.logger.Err(err).Msg("could not unmarshal heartbeat")
		return
	}

	if err := p.wsc.SendJSON(struct {
		Pong uint64 `json:"pong"`
	}{Pong: heartbeat.Ping}); err != nil {
		p.logger.Err(err).Msg("could not send pong message back")
	}
}

func (p *HuobiProvider) setTicker(tradeMsg HuobiWsTradeMsg) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	symbol := tradeMsg.Channel
	price := tradeMsg.Tick.Data[0].Price
	timestamp := tradeMsg.Tick.Data[0].Time

	if ticker, ok := p.tickers[symbol]; ok {
		ticker.Price = price
		ticker.Time = timestamp
		p.tickers[symbol] = ticker
	} else {
		p.tickers[symbol] = HuobiTicker{
			Price:  price,
			Volume: 0,
			Time:   timestamp,
		}
	}
}

func (p *HuobiProvider) setTickerVolume(symbol string, volume float64) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if ticker, ok := p.tickers[symbol]; ok {
		ticker.Volume = volume
		p.tickers[symbol] = ticker
	}
}

// GetAvailablePairs returns all pairs to which the provider can subscribe.
func (p *HuobiProvider) GetAvailablePairs() (map[string]struct{}, error) {
	resp, err := http.Get(p.endpoints.Rest + huobiRestPath)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pairsSummary HuobiPairsSummary
	if err := json.NewDecoder(resp.Body).Decode(&pairsSummary); err != nil {
		return nil, err
	}

	availablePairs := make(map[string]struct{}, len(pairsSummary.Data))
	for _, pair := range pairsSummary.Data {
		availablePairs[strings.ToUpper(pair.Symbol)] = struct{}{}
	}

	return availablePairs, nil
}

// decompressGzip uncompress gzip compressed messages. All data returned from the
// websocket Market APIs is compressed with GZIP, so it needs to be unzipped.
func decompressGzip(bz []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(bz))
	if err != nil {
		return nil, err
	}

	return io.ReadAll(r)
}
