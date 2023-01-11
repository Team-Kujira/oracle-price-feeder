package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"price-feeder/oracle/types"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	lbankWSHost     = "www.lbkex.net"
	lbankWSPath     = "/ws/V2/"
	lbankRestHost   = "api.lbkex.com"
	lbankTimeFormat = "2006-01-02T15:04:05.000"
)

var _ Provider = (*LbankProvider)(nil)

type (
	LbankProvider struct {
		wsc             *WebsocketController
		logger          zerolog.Logger
		mtx             sync.Mutex
		endpoints       Endpoint
		tickers         map[string]LbankTickerMsg
		subscribedPairs map[string]types.CurrencyPair
	}

	LbankSubscriptionMsg struct {
		Action    string `json:"action"`
		Subscribe string `json:"subscribe"`
		Pair      string `json:"pair"`
	}

	LbankTickerMsg struct {
		Server string      `json:"SERVER"`
		Tick   LbankTicker `json:"tick"`
		Type   string      `json:"type"`
		Pair   string      `json:"pair"`
		Time   string      `json:"TS"`
	}

	LbankTicker struct {
		Volume float64 `json:"vol"`
		Price  float64 `json:"latest"`
	}

	LbankPingMsg struct {
		Action string `json:"action"`
		Ping   string `json:"ping"`
	}

	LbankPongMsg struct {
		Action string `json:"action"`
		Pong   string `json:"pong"`
	}
)

func NewLbankProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*LbankProvider, error) {
	if endpoints.Name != ProviderLbank {
		endpoints = Endpoint{
			Name:      ProviderLbank,
			Rest:      lbankRestHost,
			Websocket: lbankWSHost,
		}
	}

	wsURL := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   lbankWSPath,
	}

	lbankLogger := logger.With().Str("provider", string(ProviderLbank)).Logger()

	provider := &LbankProvider{
		logger:          lbankLogger,
		endpoints:       endpoints,
		tickers:         map[string]LbankTickerMsg{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	setSubscribedPairs(provider, pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderLbank,
		wsURL,
		provider.GetSubscriptionMsgs(pairs...),
		provider.messageReceived,
		defaultPingDuration,
		websocket.TextMessage,
		lbankLogger,
	)

	provider.wsc.pingMessage = `{"action":"ping","ping":"1"}`

	go provider.wsc.Start()

	return provider, nil
}

func (p *LbankProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, len(cps))

	for i, cp := range cps {
		subscriptionMsgs[i] = LbankSubscriptionMsg{
			Action:    "subscribe",
			Subscribe: "tick",
			Pair:      strings.ToLower(cp.Join("_")),
		}
	}

	return subscriptionMsgs
}

func (p *LbankProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	cp, ok := p.subscribedPairs[s]
	return cp, ok
}

func (p *LbankProvider) SetSubscribedPair(cp types.CurrencyPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.subscribedPairs[cp.String()] = cp
}

func (p *LbankProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	return subscribeCurrencyPairs(p, cps)
}

func (p *LbankProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return p.wsc.AddSubscriptionMsgs(msgs)
}

func (p *LbankProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	return getTickerPrices(p, cps)
}

func (p *LbankProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	key := strings.ToLower(cp.Join("_"))

	ticker, ok := p.tickers[key]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf("lbank failed to get ticker price for %s", key)
	}

	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return types.TickerPrice{}, fmt.Errorf("lbank failed to load time zone for %s", key)
	}

	timestamp, err := time.ParseInLocation(lbankTimeFormat, ticker.Time, loc)
	if err != nil {
		return types.TickerPrice{}, fmt.Errorf("lbank failed to parse timestamp for %s", key)
	}

	return types.NewTickerPrice(
		string(ProviderLbank),
		cp.String(),
		fmt.Sprintf("%f", ticker.Tick.Price),
		fmt.Sprintf("%f", ticker.Tick.Volume),
		timestamp.UnixMilli(),
	)
}

func (p *LbankProvider) messageReceived(messageType int, bz []byte) {
	var (
		tickerMsg LbankTickerMsg
		tickerErr error
		pingMsg   LbankPingMsg
		pongMsg   LbankPongMsg
	)

	tickerErr = json.Unmarshal(bz, &tickerMsg)
	// symbol for spot ticker has a prefix "s", e.x.: "sBTCUSDT"
	if tickerErr == nil && tickerMsg.Type == "tick" {
		p.setTickerPair(tickerMsg)
		telemetryWebsocketMessage(ProviderLbank, MessageTypeTicker)
		return
	}

	err := json.Unmarshal(bz, &pingMsg)
	if err == nil && pingMsg.Ping != "" {
		p.pong(pingMsg.Ping)
		return
	}

	err = json.Unmarshal(bz, &pongMsg)
	if err == nil && pongMsg.Pong == "1" {
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		Str("msg", string(bz)).
		AnErr("ticker", tickerErr).
		Msg("Error on receive message")
}

// pong returns a pong message when a "ping" is received and resets the
// reconnect ticker because the connection is alive. After connected to Lbank's
// websocket server, the server will send ping messages periodically.
// When client receives a ping message, it should respond with a matching
// "pong" message with the same id as the ping message.
// ping: { "action":"ping", "ping":"0ca8f854-7ba7-4341-9d86-d3327e52804e" }
// pong: { "action":"pong", "pong":"0ca8f854-7ba7-4341-9d86-d3327e52804e" }
func (p *LbankProvider) pong(ping string) {
	pong := LbankPongMsg{
		Action: "pong",
		Pong:   ping,
	}

	if err := p.wsc.SendJSON(pong); err != nil {
		p.logger.Err(err).Msg("could not send pong message back")
	}
}

func (p *LbankProvider) setTickerPair(ticker LbankTickerMsg) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.tickers[ticker.Pair] = ticker
}

func (p *LbankProvider) GetAvailablePairs() (map[string]struct{}, error) {
	// not used yet, so skipping this unless needed
	return make(map[string]struct{}, 0), nil
}
