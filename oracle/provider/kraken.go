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

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	krakenWSHost                  = "ws.kraken.com"
	KrakenRestHost                = "https://api.kraken.com"
	KrakenRestPath                = "/0/public/AssetPairs"
	krakenEventSystemStatus       = "systemStatus"
	krakenEventSubscriptionStatus = "subscriptionStatus"
)

var _ Provider = (*KrakenProvider)(nil)

type (
	// KrakenProvider defines an Oracle provider implemented by the Kraken public
	// API.
	//
	// REF: https://docs.kraken.com/websockets/#overview
	KrakenProvider struct {
		wsc             *WebsocketController
		logger          zerolog.Logger
		mtx             sync.RWMutex
		endpoints       Endpoint
		tickers         map[string]types.TickerPrice  // Symbol => TickerPrice
		subscribedPairs map[string]types.CurrencyPair // Symbol => types.CurrencyPair
	}

	// KrakenTicker ticker price response from Kraken ticker channel.
	// REF: https://docs.kraken.com/websockets/#message-ticker
	KrakenTicker struct {
		C    []string `json:"c"` // Close with Price in the first position
		V    []string `json:"v"` // Volume with the value over last 24 hours in the second position
		Time uint64   // Not received from ws, need to be set manually
	}

	// KrakenSubscriptionMsg Msg to subscribe to all the pairs at once.
	KrakenSubscriptionMsg struct {
		Event        string                    `json:"event"`        // subscribe/unsubscribe
		Pair         []string                  `json:"pair"`         // Array of currency pairs ex.: "BTC/USDT",
		Subscription KrakenSubscriptionChannel `json:"subscription"` // subscription object
	}

	// KrakenSubscriptionChannel Msg with the channel name to be subscribed.
	KrakenSubscriptionChannel struct {
		Name string `json:"name"` // channel to be subscribed ex.: ticker
	}

	// KrakenEvent wraps the possible events from the provider.
	KrakenEvent struct {
		Event string `json:"event"` // events from kraken ex.: systemStatus | subscriptionStatus
	}

	// KrakenEventSubscriptionStatus parse the subscriptionStatus event message.
	KrakenEventSubscriptionStatus struct {
		Status       string `json:"status"`       // subscribed|unsubscribed|error
		Pair         string `json:"pair"`         // Pair symbol base/quote ex.: "XBT/USD"
		ErrorMessage string `json:"errorMessage"` // error description
	}

	// KrakenPairsSummary defines the response structure for an Kraken pairs summary.
	KrakenPairsSummary struct {
		Result map[string]KrakenPairData `json:"result"`
	}

	// KrakenPairData defines the data response structure for an Kraken pair.
	KrakenPairData struct {
		WsName string `json:"wsname"`
	}
)

// NewKrakenProvider returns a new Kraken provider with the WS connection and msg handler.
func NewKrakenProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*KrakenProvider, error) {
	if endpoints.Name != ProviderKraken {
		endpoints = Endpoint{
			Name:      ProviderKraken,
			Rest:      KrakenRestHost,
			Websocket: krakenWSHost,
		}
	}

	wsURL := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
	}

	krakenLogger := logger.With().Str("provider", string(ProviderKraken)).Logger()

	provider := &KrakenProvider{
		logger:          krakenLogger,
		endpoints:       endpoints,
		tickers:         map[string]types.TickerPrice{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	provider.setSubscribedPairs(pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderKraken,
		wsURL,
		provider.getSubscriptionMsgs(pairs...),
		provider.messageReceived,
		disabledPingDuration,
		websocket.PingMessage,
		krakenLogger,
	)
	go provider.wsc.Start()

	return provider, nil
}

func (p *KrakenProvider) getSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, 1)

	pairs := []string{}

	for _, cp := range cps {
		pairs = append(pairs, strings.ToUpper(cp.Join("/")))
	}

	subscriptionMsgs[0] = KrakenSubscriptionMsg{
		Event: "subscribe",
		Pair:  pairs,
		Subscription: KrakenSubscriptionChannel{
			Name: "ticker",
		},
	}

	return subscriptionMsgs
}

// SubscribeCurrencyPairs sends the new subscription messages to the websocket
// and adds them to the providers subscribedPairs array
func (p *KrakenProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
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
func (p *KrakenProvider) GetTickerPrices(pairs ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	tickerPrices := make(map[string]types.TickerPrice, len(pairs))

	for _, cp := range pairs {
		key := cp.String()
		tickerPrice, ok := p.tickers[key]
		if !ok {
			return nil, fmt.Errorf("kraken failed to get ticker price for %s", key)
		}
		tickerPrices[key] = tickerPrice
	}

	return tickerPrices, nil
}

// messageReceived handles any message sent by the provider.
func (p *KrakenProvider) messageReceived(messageType int, bz []byte) {
	if messageType != websocket.TextMessage {
		return
	}

	var (
		krakenEvent KrakenEvent
		krakenErr   error
		tickerErr   error
	)

	krakenErr = json.Unmarshal(bz, &krakenEvent)
	if krakenErr == nil {
		switch krakenEvent.Event {
		case krakenEventSystemStatus:
			return
		case krakenEventSubscriptionStatus:
			p.messageReceivedSubscriptionStatus(bz)
			return
		}
		return
	}

	tickerErr = p.messageReceivedTickerPrice(bz)
	if tickerErr == nil {
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("ticker", tickerErr).
		AnErr("event", krakenErr).
		Msg("Error on receive message")
}

// messageReceivedTickerPrice handles the ticker price msg.
func (p *KrakenProvider) messageReceivedTickerPrice(bz []byte) error {
	// the provider response is an array with different types at each index
	// kraken documentation https://docs.kraken.com/websockets/#message-ticker
	var tickerMessage []interface{}
	if err := json.Unmarshal(bz, &tickerMessage); err != nil {
		return err
	}

	if len(tickerMessage) != 4 {
		return fmt.Errorf("received an unexpected structure")
	}

	channelName, ok := tickerMessage[2].(string)
	if !ok || channelName != "ticker" {
		return fmt.Errorf("received an unexpected channel name")
	}

	tickerBz, err := json.Marshal(tickerMessage[1])
	if err != nil {
		p.logger.Err(err).Msg("could not marshal ticker message")
		return err
	}

	var krakenTicker KrakenTicker
	if err := json.Unmarshal(tickerBz, &krakenTicker); err != nil {
		p.logger.Err(err).Msg("could not unmarshal ticker message")
		return err
	}

	krakenPair, ok := tickerMessage[3].(string)
	if !ok {
		p.logger.Debug().Msg("received an unexpected pair")
		return err
	}

	krakenPair = normalizeKrakenBTCPair(krakenPair)
	currencyPairSymbol := krakenPairToCurrencyPairSymbol(krakenPair)

	tickerPrice, err := krakenTicker.toTickerPrice(currencyPairSymbol)
	if err != nil {
		p.logger.Err(err).Msg("could not parse kraken ticker to ticker price")
		return err
	}

	p.setTickerPair(currencyPairSymbol, tickerPrice)
	telemetryWebsocketMessage(ProviderKraken, MessageTypeTicker)
	return nil
}

// messageReceivedSubscriptionStatus handle the subscription status message
// sent by the provider.
func (p *KrakenProvider) messageReceivedSubscriptionStatus(bz []byte) {
	var subscriptionStatus KrakenEventSubscriptionStatus
	if err := json.Unmarshal(bz, &subscriptionStatus); err != nil {
		p.logger.Err(err).Msg("provider could not unmarshal KrakenEventSubscriptionStatus")
		return
	}

	switch subscriptionStatus.Status {
	case "error":
		p.logger.Error().Msg(subscriptionStatus.ErrorMessage)
		p.removeSubscribedTickers(krakenPairToCurrencyPairSymbol(subscriptionStatus.Pair))
		return
	case "unsubscribed":
		p.logger.Debug().Msgf("ticker %s was unsubscribed", subscriptionStatus.Pair)
		p.removeSubscribedTickers(krakenPairToCurrencyPairSymbol(subscriptionStatus.Pair))
		return
	}
}

// setTickerPair sets an ticker to the map thread safe by the mutex.
func (p *KrakenProvider) setTickerPair(symbol string, ticker types.TickerPrice) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	ticker.Time = time.Now().UnixMilli()
	fmt.Println(ticker)
	p.tickers[symbol] = ticker
}

// setSubscribedPairs sets N currency pairs to the map of subscribed pairs.
func (p *KrakenProvider) setSubscribedPairs(cps ...types.CurrencyPair) {
	for _, cp := range cps {
		p.subscribedPairs[cp.String()] = cp
	}
}

// removeSubscribedTickers delete N pairs from the subscribed map.
func (p *KrakenProvider) removeSubscribedTickers(tickerSymbols ...string) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	for _, tickerSymbol := range tickerSymbols {
		delete(p.subscribedPairs, tickerSymbol)
	}
}

// GetAvailablePairs returns all pairs to which the provider can subscribe.
func (p *KrakenProvider) GetAvailablePairs() (map[string]struct{}, error) {
	resp, err := http.Get(p.endpoints.Rest + KrakenRestPath)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pairsSummary KrakenPairsSummary
	if err := json.NewDecoder(resp.Body).Decode(&pairsSummary); err != nil {
		return nil, err
	}

	availablePairs := make(map[string]struct{}, len(pairsSummary.Result))
	for _, pair := range pairsSummary.Result {
		splitPair := strings.Split(pair.WsName, "/")
		if len(splitPair) != 2 {
			continue
		}

		cp := types.CurrencyPair{
			Base:  strings.ToUpper(splitPair[0]),
			Quote: strings.ToUpper(splitPair[1]),
		}
		availablePairs[cp.String()] = struct{}{}
	}

	return availablePairs, nil
}

// toTickerPrice return a TickerPrice based on the KrakenTicker.
func (ticker KrakenTicker) toTickerPrice(symbol string) (types.TickerPrice, error) {
	if len(ticker.C) != 2 || len(ticker.V) != 2 {
		return types.TickerPrice{}, fmt.Errorf("error converting KrakenTicker to TickerPrice")
	}
	// ticker.C has the Price in the first position.
	// ticker.V has the 24h volume in the second position.
	return types.NewTickerPrice(
		string(ProviderKraken),
		symbol,
		ticker.C[0],
		ticker.V[1],
		int64(ticker.Time),
	)
}

// krakenPairToCurrencyPairSymbol receives a kraken pair formated
// ex.: ATOM/USDT and return currencyPair Symbol ATOMUSDT.
func krakenPairToCurrencyPairSymbol(krakenPair string) string {
	return strings.ReplaceAll(krakenPair, "/", "")
}

// normalizeKrakenBTCPair changes XBT pairs to BTC,
// since other providers list bitcoin as BTC.
func normalizeKrakenBTCPair(ticker string) string {
	return strings.Replace(ticker, "XBT", "BTC", 1)
}
