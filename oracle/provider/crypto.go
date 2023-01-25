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

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	cryptoWSHost             = "stream.crypto.com"
	cryptoWSPath             = "/v2/market"
	cryptoReconnectTime      = time.Second * 30
	cryptoRestHost           = "https://api.crypto.com"
	cryptoRestPath           = "/v2/public/get-ticker"
	cryptoTickerChannel      = "ticker"
	cryptoCandleChannel      = "candlestick"
	cryptoHeartbeatMethod    = "public/heartbeat"
	cryptoHeartbeatReqMethod = "public/respond-heartbeat"
	cryptoTickerMsgPrefix    = "ticker."
	cryptoCandleMsgPrefix    = "candlestick.5m."
)

var _ Provider = (*CryptoProvider)(nil)

type (
	// CryptoProvider defines an Oracle provider implemented by the Crypto.com public
	// API.
	//
	// REF: https://exchange-docs.crypto.com/spot/index.html#introduction
	CryptoProvider struct {
		wsc             *WebsocketController
		logger          zerolog.Logger
		mtx             sync.RWMutex
		endpoints       Endpoint
		tickers         map[string]CryptoTicker       // Symbol => TickerPrice
		subscribedPairs map[string]types.CurrencyPair // Symbol => types.CurrencyPair
	}

	CryptoWsSubscriptionMsg struct {
		ID     int64                      `json:"id"`
		Method string                     `json:"method"` // subscribe, unsubscribe
		Params CryptoWsSubscriptionParams `json:"params"`
		Nonce  int64                      `json:"nonce"` // Current timestamp (milliseconds since the Unix epoch)
	}

	CryptoWsSubscriptionParams struct {
		Channels []string `json:"channels"` // Channels to be subscribed ex. ticker.ATOM_USDT
	}

	CryptoWsCandleMsg struct {
		ID     int64                `json:"id"`
		Result CryptoWsCandleResult `json:"result"`
	}

	CryptoWsCandleResult struct {
		Channel string               `json:"channel"`
		Symbol  string               `json:"instrument_name"`
		Data    []CryptoWsCandleData `json:"data"`
	}

	CryptoWsCandleData struct {
		Time  int64  `json:"ut"`
		Close string `json:"c"`
	}

	CryptoWsHeartbeatMsg struct {
		ID     int64  `json:"id"`
		Method string `json:"method"` // public/heartbeat
	}

	CryptoRestTickerResponse struct {
		Code int64                  `json:"code"`
		Data []CryptoRestTickerData `json:"data"`
	}

	CryptoRestTickerData struct {
		Symbol string `json:"i"`
		Volume string `json:"v"`
	}

	CryptoTicker struct {
		Price  string
		Volume string
		Time   int64
	}
)

func NewCryptoProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*CryptoProvider, error) {
	if endpoints.Name != ProviderCrypto {
		endpoints = Endpoint{
			Name:      ProviderCrypto,
			Rest:      cryptoRestHost,
			Websocket: cryptoWSHost,
		}
	}

	wsURL := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   cryptoWSPath,
	}

	cryptoLogger := logger.With().Str("provider", "crypto").Logger()

	provider := &CryptoProvider{
		logger:          cryptoLogger,
		endpoints:       endpoints,
		tickers:         map[string]CryptoTicker{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	setSubscribedPairs(provider, pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderCrypto,
		wsURL,
		provider.GetSubscriptionMsgs(pairs...),
		provider.messageReceived,
		disabledPingDuration,
		websocket.PingMessage,
		cryptoLogger,
	)
	go provider.wsc.Start()

	return provider, nil
}

func (p *CryptoProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, 1)

	channels := make([]string, len(cps))

	for i, cp := range cps {
		if cp.Base == "LUNA" {
			channels[i] = "candlestick.M1.LUNA2_" + cp.Quote
		} else {
			channels[i] = "candlestick.M1." + cp.Join("_")
		}
	}

	subscriptionMsgs[0] = CryptoWsSubscriptionMsg{
		ID:     1,
		Method: "subscribe",
		Params: CryptoWsSubscriptionParams{
			Channels: channels,
		},
		Nonce: time.Now().UnixMilli(),
	}

	return subscriptionMsgs
}

func (p *CryptoProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	cp, ok := p.subscribedPairs[s]
	return cp, ok
}

func (p *CryptoProvider) SetSubscribedPair(cp types.CurrencyPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.subscribedPairs[cp.String()] = cp
}

func (p *CryptoProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	return subscribeCurrencyPairs(p, cps)
}

func (p *CryptoProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return p.wsc.AddSubscriptionMsgs(msgs)
}

// GetTickerPrices returns the tickerPrices based on the saved map.
func (p *CryptoProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	go func(p *CryptoProvider) {
		requiredSymbols := make(map[string]bool, len(cps))

		for _, cp := range cps {
			symbol := strings.ToLower(cp.String())

			if cp.Base == "LUNA" {
				symbol = "LUNA2_" + cp.Quote
			}

			requiredSymbols[symbol] = true
		}

		resp, err := http.Get(p.endpoints.Rest + "/v2/public/get-ticker")
		if err != nil {
			p.logger.Error().Msg("failed to get tickers")
			return
		}
		defer resp.Body.Close()

		var tickerResp CryptoRestTickerResponse
		err = json.NewDecoder(resp.Body).Decode(&tickerResp)
		if err != nil {
			p.logger.Error().Msg("failed to parse rest response")
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

func (p *CryptoProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	key := cp.Join("_")

	if cp.Base == "LUNA" {
		key = "LUNA2_" + cp.Quote
	}

	ticker, ok := p.tickers[key]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf(
			types.ErrTickerNotFound.Error(),
			ProviderCrypto,
			key,
		)
	}

	return types.TickerPrice{
		Price:  sdk.MustNewDecFromStr(ticker.Price),
		Volume: sdk.MustNewDecFromStr(ticker.Volume),
		Time:   ticker.Time,
	}, nil
}

func (p *CryptoProvider) messageReceived(messageType int, bz []byte) {
	if messageType != websocket.TextMessage {
		return
	}

	var (
		heartbeatResp CryptoWsHeartbeatMsg
		heartbeatErr  error
		candleMsg     CryptoWsCandleMsg
		candleErr     error
	)

	// sometimes the message received is not a ticker or a candle response.
	heartbeatErr = json.Unmarshal(bz, &heartbeatResp)
	if heartbeatResp.Method == cryptoHeartbeatMethod {
		p.pong(heartbeatResp)
		return
	}

	candleErr = json.Unmarshal(bz, &candleMsg)
	if candleErr == nil && len(candleMsg.Result.Data) > 0 {
		p.setTickerPrice(candleMsg)
		telemetryWebsocketMessage(ProviderCrypto, MessageTypeCandle)
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("heartbeat", heartbeatErr).
		AnErr("candle", candleErr).
		Str("msg", string(bz)).
		Msg("Error on receive message")
}

// pong return a heartbeat message when a "ping" is received and reset the
// recconnect ticker because the connection is alive. After connected to crypto.com's
// Websocket server, the server will send heartbeat periodically (30s interval).
// When client receives an heartbeat message, it must respond back with the
// public/respond-heartbeat method, using the same matching id,
// within 5 seconds, or the connection will break.
func (p *CryptoProvider) pong(heartbeatResp CryptoWsHeartbeatMsg) {
	heartbeatReq := CryptoWsHeartbeatMsg{
		ID:     heartbeatResp.ID,
		Method: cryptoHeartbeatReqMethod,
	}

	if err := p.wsc.SendJSON(heartbeatReq); err != nil {
		p.logger.Err(err).Msg("could not send pong message back")
	}
}

func (p *CryptoProvider) setTickerPrice(candleMsg CryptoWsCandleMsg) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	symbol := candleMsg.Result.Symbol
	data := candleMsg.Result.Data[len(candleMsg.Result.Data)-1]
	price := data.Close
	timestamp := data.Time

	if ticker, ok := p.tickers[symbol]; ok {
		ticker.Price = price
		ticker.Time = timestamp
		p.tickers[symbol] = ticker
	} else {
		p.tickers[symbol] = CryptoTicker{
			Price:  price,
			Volume: "0",
			Time:   timestamp,
		}
	}
}

func (p *CryptoProvider) setTickerVolume(symbol string, volume string) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if ticker, ok := p.tickers[symbol]; ok {
		ticker.Volume = volume
		p.tickers[symbol] = ticker
	}
}

// GetAvailablePairs returns all pairs to which the provider can subscribe.
// ex.: map["ATOMUSDT" => {}, "UMEEUSDC" => {}].
func (p *CryptoProvider) GetAvailablePairs() (map[string]struct{}, error) {
	// not used yet, so skipping this unless needed
	return make(map[string]struct{}, 0), nil
}
