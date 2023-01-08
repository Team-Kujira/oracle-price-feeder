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
	gateWSHost    = "ws.gate.io"
	gateWSPath    = "/v4"
	gatePingCheck = time.Second * 25 // should be < 30
	gateRestHost  = "https://api.gateio.ws"
	gateRestPath  = "/api/v4/spot/currency_pairs"
)

var _ Provider = (*GateProvider)(nil)

type (
	// GateProvider defines an Oracle provider implemented by the Gate public
	// API.
	//
	// REF: https://www.gate.io/docs/websocket/index.html
	GateProvider struct {
		wsc             *WebsocketController
		logger          zerolog.Logger
		reconnectTimer  *time.Ticker
		mtx             sync.RWMutex
		endpoints       Endpoint
		tickers         map[string]GateTicker         // Symbol => GateTicker
		subscribedPairs map[string]types.CurrencyPair // Symbol => types.CurrencyPair
	}

	GateTicker struct {
		Last   string `json:"last"`        // Last traded price ex.: 43508.9
		Vol    string `json:"quoteVolume"` // Trading volume ex.: 11159.87127845
		Symbol string `json:"symbol"`      // Symbol ex.: ATOM_UDST
		Time   int64  // Timestamp, not provided by API
	}

	// GateTickerSubscriptionMsg Msg to subscribe all the tickers channels.
	GateTickerSubscriptionMsg struct {
		Method string   `json:"method"` // ticker.subscribe
		Params []string `json:"params"` // streams to subscribe ex.: BOT_USDT
		ID     uint16   `json:"id"`     // identify messages going back and forth
	}

	// GateTickerResponse defines the response body for gate tickers.
	GateTickerResponse struct {
		Method string        `json:"method"`
		Params []interface{} `json:"params"`
	}

	// GateEvent defines the response body for gate subscription statuses.
	GateEvent struct {
		ID     int             `json:"id"`     // subscription id, ex.: 123
		Result GateEventResult `json:"result"` // event result body
	}
	// GateEventResult defines the Result body for the GateEvent response.
	GateEventResult struct {
		Status string `json:"status"` // ex. "successful"
	}

	// GatePairSummary defines the response structure for a Gate pair summary.
	GatePairSummary struct {
		Base  string `json:"base"`
		Quote string `json:"quote"`
	}
)

// NewGateProvider creates a new GateProvider.
func NewGateProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*GateProvider, error) {
	if endpoints.Name != ProviderGate {
		endpoints = Endpoint{
			Name:      ProviderGate,
			Rest:      gateRestHost,
			Websocket: gateWSHost,
		}
	}

	wsURL := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   gateWSPath,
	}

	gateLogger := logger.With().Str("provider", string(ProviderGate)).Logger()

	provider := &GateProvider{
		logger:          gateLogger,
		reconnectTimer:  time.NewTicker(gatePingCheck),
		endpoints:       endpoints,
		tickers:         map[string]GateTicker{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	provider.setSubscribedPairs(pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderGate,
		wsURL,
		provider.getSubscriptionMsgs(pairs...),
		provider.messageReceived,
		defaultPingDuration,
		websocket.PingMessage,
		gateLogger,
	)
	go provider.wsc.Start()

	return provider, nil
}

func (p *GateProvider) getSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, 1)

	params := make([]string, len(cps))

	for i, cp := range cps {
		params[i] = strings.ToUpper(cp.Join("_"))
	}

	subscriptionMsgs[0] = GateTickerSubscriptionMsg{
		Method: "ticker.subscribe",
		Params: params,
		ID:     1,
	}

	return subscriptionMsgs
}

// SubscribeCurrencyPairs sends the new subscription messages to the websocket
// and adds them to the providers subscribedPairs array
func (p *GateProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
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
func (p *GateProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	return getTickerPrices(p, cps)
}

func (p *GateProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	gp := strings.ToUpper(cp.Join("_"))
	if tickerPair, ok := p.tickers[gp]; ok {
		return tickerPair.toTickerPrice()
	}

	return types.TickerPrice{}, fmt.Errorf("gate failed to get ticker price for %s", gp)
}

func (p *GateProvider) messageReceived(messageType int, bz []byte) {
	var (
		gateEvent GateEvent
		gateErr   error
		tickerErr error
	)

	gateErr = json.Unmarshal(bz, &gateEvent)
	if gateErr == nil {
		switch gateEvent.Result.Status {
		case "success":
			return
		case "":
			break
		default:
			return
		}
	}

	tickerErr = p.messageReceivedTickerPrice(bz)
	if tickerErr == nil {
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("ticker", tickerErr).
		AnErr("event", gateErr).
		Msg("Error on receive message")
}

// messageReceivedTickerPrice handles the ticker price msg.
// The provider response is a slice with different types at each index.
//
// REF: https://www.gate.io/docs/websocket/index.html
func (p *GateProvider) messageReceivedTickerPrice(bz []byte) error {
	var tickerMessage GateTickerResponse
	if err := json.Unmarshal(bz, &tickerMessage); err != nil {
		return err
	}

	if tickerMessage.Method != "ticker.update" {
		return fmt.Errorf("message is not a ticker update")
	}

	tickerBz, err := json.Marshal(tickerMessage.Params[1])
	if err != nil {
		p.logger.Err(err).Msg("could not marshal ticker message")
		return err
	}

	var gateTicker GateTicker
	if err := json.Unmarshal(tickerBz, &gateTicker); err != nil {
		p.logger.Err(err).Msg("could not unmarshal ticker message")
		return err
	}

	symbol, ok := tickerMessage.Params[0].(string)
	if !ok {
		return fmt.Errorf("symbol should be a string")
	}
	gateTicker.Symbol = symbol

	p.setTickerPair(gateTicker)
	telemetryWebsocketMessage(ProviderGate, MessageTypeTicker)
	return nil
}

func (p *GateProvider) setTickerPair(ticker GateTicker) {
	ticker.Time = time.Now().UnixMilli()

	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.tickers[ticker.Symbol] = ticker
}

// setSubscribedPairs sets N currency pairs to the map of subscribed pairs.
func (p *GateProvider) setSubscribedPairs(cps ...types.CurrencyPair) {
	for _, cp := range cps {
		p.subscribedPairs[cp.String()] = cp
	}
}

// GetAvailablePairs returns all pairs to which the provider can subscribe.
func (p *GateProvider) GetAvailablePairs() (map[string]struct{}, error) {
	resp, err := http.Get(p.endpoints.Rest + gateRestPath)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pairsSummary []GatePairSummary
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

func (ticker GateTicker) toTickerPrice() (types.TickerPrice, error) {
	return types.NewTickerPrice(
		string(ProviderGate),
		ticker.Symbol,
		ticker.Last,
		ticker.Vol,
		ticker.Time,
	)
}
