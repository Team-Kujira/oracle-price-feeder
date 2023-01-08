package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"

	"price-feeder/oracle/types"
)

const (
	okxWSHost   = "ws.okx.com:8443"
	okxWSPath   = "/ws/v5/public"
	okxRestHost = "https://www.okx.com"
	okxRestPath = "/api/v5/market/tickers?instType=SPOT"
)

var _ Provider = (*OkxProvider)(nil)

type (
	// OkxProvider defines an Oracle provider implemented by the Okx public
	// API.
	//
	// REF: https://www.okx.com/docs-v5/en/#websocket-api-public-channel-tickers-channel
	OkxProvider struct {
		wsc             *WebsocketController
		logger          zerolog.Logger
		mtx             sync.RWMutex
		endpoints       Endpoint
		tickers         map[string]OkxTickerPair      // InstId => OkxTickerPair
		subscribedPairs map[string]types.CurrencyPair // Symbol => types.CurrencyPair
	}

	// OkxInstId defines the id Symbol of an pair.
	OkxInstID struct {
		InstID string `json:"instId"` // Instrument ID ex.: BTC-USDT
	}

	// OkxTickerPair defines a ticker pair of Okx.
	OkxTickerPair struct {
		OkxInstID
		Last   string `json:"last"`   // Last traded price ex.: 43508.9
		Vol24h string `json:"vol24h"` // 24h trading volume ex.: 11159.87127845
		TS     string `json:"ts"`     // Timestamp
	}

	// OkxInst defines the structure containing ID information for the OkxResponses.
	OkxID struct {
		OkxInstID
		Channel string `json:"channel"`
	}

	// OkxTickerResponse defines the response structure of a Okx ticker request.
	OkxTickerResponse struct {
		Data []OkxTickerPair `json:"data"`
		ID   OkxID           `json:"arg"`
	}

	// OkxSubscriptionTopic Topic with the ticker to be subscribed/unsubscribed.
	OkxSubscriptionTopic struct {
		Channel string `json:"channel"` // Channel name ex.: tickers
		InstID  string `json:"instId"`  // Instrument ID ex.: BTC-USDT
	}

	// OkxSubscriptionMsg Message to subscribe/unsubscribe with N Topics.
	OkxSubscriptionMsg struct {
		Op   string                 `json:"op"` // Operation ex.: subscribe
		Args []OkxSubscriptionTopic `json:"args"`
	}

	// OkxPairsSummary defines the response structure for an Okx pairs summary.
	OkxPairsSummary struct {
		Data []OkxInstID `json:"data"`
	}
)

// NewOkxProvider creates a new OkxProvider.
func NewOkxProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*OkxProvider, error) {
	if endpoints.Name != ProviderOkx {
		endpoints = Endpoint{
			Name:      ProviderOkx,
			Rest:      okxRestHost,
			Websocket: okxWSHost,
		}
	}

	wsURL := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   okxWSPath,
	}

	okxLogger := logger.With().Str("provider", string(ProviderOkx)).Logger()

	provider := &OkxProvider{
		logger:          okxLogger,
		endpoints:       endpoints,
		tickers:         map[string]OkxTickerPair{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	provider.setSubscribedPairs(pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderOkx,
		wsURL,
		provider.getSubscriptionMsgs(pairs...),
		provider.messageReceived,
		defaultPingDuration,
		websocket.PingMessage,
		okxLogger,
	)
	go provider.wsc.Start()

	return provider, nil
}

func (p *OkxProvider) getSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, 1)

	okxTopics := []OkxSubscriptionTopic{}

	for _, cp := range cps {
		okxTopics = append(okxTopics, OkxSubscriptionTopic{
			Channel: "tickers",
			InstID:  strings.ToUpper(cp.Join("-")),
		})
	}

	subscriptionMsgs[0] = OkxSubscriptionMsg{
		Op:   "subscribe",
		Args: okxTopics,
	}

	return subscriptionMsgs
}

// SubscribeCurrencyPairs sends the new subscription messages to the websocket
// and adds them to the providers subscribedPairs array
func (p *OkxProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
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
func (p *OkxProvider) GetTickerPrices(pairs ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	tickerPrices := make(map[string]types.TickerPrice, len(pairs))

	for _, currencyPair := range pairs {
		price, err := p.GetTickerPrice(currencyPair)
		if err != nil {
			return nil, err
		}

		tickerPrices[currencyPair.String()] = price
	}

	return tickerPrices, nil
}

func (p *OkxProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	instrumentID := strings.ToUpper(cp.Join("-"))
	tickerPair, ok := p.tickers[instrumentID]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf("okx failed to get ticker price for %s", instrumentID)
	}

	return tickerPair.toTickerPrice()
}

func (p *OkxProvider) messageReceived(messageType int, bz []byte) {
	var (
		tickerResp OkxTickerResponse
		tickerErr  error
	)

	// sometimes the message received is not a ticker or a candle response.
	tickerErr = json.Unmarshal(bz, &tickerResp)
	if tickerResp.ID.Channel == "tickers" {
		for _, tickerPair := range tickerResp.Data {
			p.setTickerPair(tickerPair)
			telemetryWebsocketMessage(ProviderOkx, MessageTypeTicker)
		}
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("ticker", tickerErr).
		Msg("Error on receive message")
}

func (p *OkxProvider) setTickerPair(tickerPair OkxTickerPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.tickers[tickerPair.InstID] = tickerPair
}

// setSubscribedPairs sets N currency pairs to the map of subscribed pairs.
func (p *OkxProvider) setSubscribedPairs(cps ...types.CurrencyPair) {
	for _, cp := range cps {
		p.subscribedPairs[cp.String()] = cp
	}
}

// GetAvailablePairs return all available pairs symbol to subscribe.
func (p *OkxProvider) GetAvailablePairs() (map[string]struct{}, error) {
	resp, err := http.Get(p.endpoints.Rest + okxRestPath)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pairsSummary struct {
		Data []OkxInstID `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&pairsSummary); err != nil {
		return nil, err
	}

	availablePairs := make(map[string]struct{}, len(pairsSummary.Data))
	for _, pair := range pairsSummary.Data {
		splitInstID := strings.Split(pair.InstID, "-")
		if len(splitInstID) != 2 {
			continue
		}

		cp := types.CurrencyPair{
			Base:  strings.ToUpper(splitInstID[0]),
			Quote: strings.ToUpper(splitInstID[1]),
		}
		availablePairs[cp.String()] = struct{}{}
	}

	return availablePairs, nil
}

func (ticker OkxTickerPair) toTickerPrice() (types.TickerPrice, error) {
	timestamp, err := strconv.Atoi(ticker.TS)
	if err != nil {
		return types.TickerPrice{}, err
	}

	return types.NewTickerPrice(
		string(ProviderOkx),
		ticker.InstID,
		ticker.Last,
		ticker.Vol24h,
		int64(timestamp),
	)
}
