package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                     Provider = (*BybitProvider)(nil)
	bybitDefaultEndpoints          = Endpoint{
		Name:          ProviderBybit,
		Rest:          "https://api.bybit.com",
		Websocket:     "stream.bybit.com",
		WebsocketPath: "/spot/public/v3",
		PingMessage:   `{"op":"ping"}`,
	}
)

type (
	BybitProvider struct {
		provider
	}

	BybitWsSubscriptionMsg struct {
		Operation string   `json:"op"`   // Operation (e.x. "subscribe")
		Args      []string `json:"args"` // Arguments to subscribe to
	}

	BybitWsGenericMsg struct {
		Operation string `json:"op"`
		Success   bool   `json:"success"`
	}

	BybitWsTickerMsg struct {
		Topic string            `json:"topic"` // e.x. "tickers.BTCUSDT"
		Data  BybitWsTickerData `json:"data"`
	}

	BybitWsTickerData struct {
		Price  string `json:"c"` // e.x. "12.3907"
		Volume string `json:"v"` // e.x. "112247.9173"
		Symbol string `json:"s"` // e.x. "BTCUSDT"
		Time   int64  `json:"t"` // e.x. 1672698023313
	}
)

// NewBybitProvider returns a new Bybit provider with the WS connection
// and msg handler.
func NewBybitProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*BybitProvider, error) {
	provider := &BybitProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		provider.messageReceived,
		provider.getSubscriptionMsgs,
	)
	return provider, nil
}

func (p *BybitProvider) getSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, 1)

	args := make([]string, len(cps))

	for i, cp := range cps {
		args[i] = "tickers." + cp.String()
	}

	subscriptionMsgs[0] = BybitWsSubscriptionMsg{
		Operation: "subscribe",
		Args:      args,
	}

	return subscriptionMsgs
}

// messageReceived handles the received data from the Bybit websocket.
func (p *BybitProvider) messageReceived(messageType int, bz []byte) {
	var (
		tickerResp  BybitWsTickerMsg
		tickerErr   error
		genericResp BybitWsGenericMsg
	)

	err := json.Unmarshal(bz, &genericResp)
	if err == nil && genericResp.Success {
		return
	}

	tickerErr = json.Unmarshal(bz, &tickerResp)
	if tickerErr == nil && tickerResp.Topic != "" {
		p.setTickerPair(tickerResp)
		telemetryWebsocketMessage(ProviderBybit, MessageTypeTicker)
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("ticker", tickerErr).
		Str("msg", string(bz)).
		Msg("Error on receive message")
}

func (p *BybitProvider) setTickerPair(ticker BybitWsTickerMsg) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	t := types.TickerPrice{
		Price:  strToDec(ticker.Data.Price),
		Volume: strToDec(ticker.Data.Volume),
		Time:   time.Unix(ticker.Data.Time, 0),
	}
	p.tickers[ticker.Data.Symbol] = t
}
