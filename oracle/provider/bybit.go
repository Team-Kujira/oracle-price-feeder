package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

const (
	bybitRestPath = "/spot/v3/public/symbols"
)

var (
	_ Provider = (*BybitProvider)(nil)
	bybitDefaultEndpoints = Endpoint{
		Name: ProviderBybit,
		Rest: "https://api.bybit.com",
		Websocket: "stream.bybit.com",
		WebsocketPath: "/spot/public/v3",
		PingMessage: `{"op":"ping"}`,
	}
)

type (
	BybitProvider struct {
		provider
	}

	BybitSubscriptionMsg struct {
		Operation string   `json:"op"`   // Operation (e.x. "subscribe")
		Args      []string `json:"args"` // Arguments to subscribe to
	}

	BybitWsGenericResponse struct {
		Operation string `json:"op"`
		Success   bool   `json:"success"`
	}

	BybitTicker struct {
		Topic string          `json:"topic"` // e.x. "tickers.BTCUSDT"
		Data  BybitTickerData `json:"data"`
	}

	BybitTickerData struct {
		Price  string `json:"c"` // e.x. "12.3907"
		Volume string `json:"v"` // e.x. "112247.9173"
		Symbol string `json:"s"` // e.x. "BTCUSDT"
		Time   int64  `json:"t"` // e.x. 1672698023313
	}

	BybitPairsSummary struct {
		RetCode uint   `json:"retCode"`
		RetMsg  string `json:"retMsg"`
		Result  BybitPairsResult
	}

	BybitPairsResult struct {
		List []BybitPairsData
	}

	BybitPairsData struct {
		Base  string `json:"baseCoin"`
		Quote string `json:"quoteCoin"`
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

	subscriptionMsgs[0] = BybitSubscriptionMsg{
		Operation: "subscribe",
		Args:      args,
	}

	return subscriptionMsgs
}

// messageReceived handles the received data from the Bybit websocket.
func (p *BybitProvider) messageReceived(messageType int, bz []byte) {
	var (
		tickerResp  BybitTicker
		tickerErr   error
		genericResp BybitWsGenericResponse
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

func (p *BybitProvider) setTickerPair(ticker BybitTicker) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	t := types.TickerPrice{
		Price: strToDec(ticker.Data.Price),
		Volume: strToDec(ticker.Data.Volume),
		Time: time.Unix(ticker.Data.Time, 0),
	}
	p.tickers[ticker.Data.Symbol] = t
}

// GetAvailablePairs returns all pairs to which the provider can subscribe.
func (p *BybitProvider) GetAvailablePairs() (map[string]struct{}, error) {
	resp, err := http.Get(p.endpoints.Rest + bybitRestPath)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pairsSummary BybitPairsSummary
	if err := json.NewDecoder(resp.Body).Decode(&pairsSummary); err != nil {
		return nil, err
	}
	if pairsSummary.RetCode != 0 {
		return nil, fmt.Errorf("unable to get bybit available pairs")
	}

	availablePairs := make(map[string]struct{}, len(pairsSummary.Result.List))
	for _, pair := range pairsSummary.Result.List {
		cp := types.CurrencyPair{
			Base:  pair.Base,
			Quote: pair.Quote,
		}
		availablePairs[cp.String()] = struct{}{}
	}

	return availablePairs, nil
}
