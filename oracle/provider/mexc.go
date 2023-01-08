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
	mexcWSHost   = "wbs.mexc.com"
	mexcWSPath   = "/raw/ws"
	mexcRestHost = "https://www.mexc.com"
	mexcRestPath = "/open/api/v2/market/ticker"
)

var _ Provider = (*MexcProvider)(nil)

type (
	// MexcProvider defines an Oracle provider implemented by the Mexc public
	// API.
	//
	// REF: https://mxcdevelop.github.io/apidocs/spot_v2_en/#ticker-information
	// REF: https://mxcdevelop.github.io/apidocs/spot_v2_en/#k-line
	// REF: https://mxcdevelop.github.io/apidocs/spot_v2_en/#overview
	MexcProvider struct {
		wsc             *WebsocketController
		logger          zerolog.Logger
		mtx             sync.RWMutex
		endpoints       Endpoint
		tickers         map[string]types.TickerPrice  // Symbol => TickerPrice
		subscribedPairs map[string]types.CurrencyPair // Symbol => types.CurrencyPair
	}

	// MexcTickerResponse is the ticker price response object.
	MexcTickerResponse struct {
		Symbol map[string]MexcTicker `json:"data"` // e.x. ATOM_USDT
	}

	MexcTicker struct {
		LastPrice float64 `json:"p"` // Last price ex.: 0.0025
		Volume    float64 `json:"v"` // Total traded base asset volume ex.: 1000
	}

	// MexcTickerSubscription Msg to subscribe all the ticker channels.
	MexcTickerSubscription struct {
		OP string `json:"op"` // kline
	}

	// MexcPairSummary defines the response structure for a Mexc pair
	// summary.
	MexcPairSummary struct {
		Symbol string `json:"symbol"`
	}
)

func NewMexcProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*MexcProvider, error) {
	if (endpoints.Name) != ProviderMexc {
		endpoints = Endpoint{
			Name:      ProviderMexc,
			Rest:      mexcRestHost,
			Websocket: mexcWSHost,
		}
	}

	wsURL := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   mexcWSPath,
	}

	mexcLogger := logger.With().Str("provider", "mexc").Logger()

	provider := &MexcProvider{
		logger:          mexcLogger,
		endpoints:       endpoints,
		tickers:         map[string]types.TickerPrice{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	provider.setSubscribedPairs(pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderMexc,
		wsURL,
		provider.getSubscriptionMsgs(pairs...),
		provider.messageReceived,
		defaultPingDuration,
		websocket.PingMessage,
		mexcLogger,
	)
	go provider.wsc.Start()

	return provider, nil
}

func (p *MexcProvider) getSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, 1)

	subscriptionMsgs[0] = MexcTickerSubscription{
		OP: "sub.overview",
	}

	return subscriptionMsgs
}

// SubscribeCurrencyPairs sends the new subscription messages to the websocket
// and adds them to the providers subscribedPairs array
func (p *MexcProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
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

// GetTickerPrices returns the tickerPrices based on the provided pairs.
func (p *MexcProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	return getTickerPrices(p, cps)
}

func (p *MexcProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	key := cp.Join("_")

	ticker, ok := p.tickers[key]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf(
			types.ErrTickerNotFound.Error(),
			ProviderMexc,
			key,
		)
	}

	return ticker, nil
}

func (p *MexcProvider) messageReceived(messageType int, bz []byte) {
	var (
		tickerResp MexcTickerResponse
		tickerErr  error
	)

	tickerErr = json.Unmarshal(bz, &tickerResp)
	for _, cp := range p.subscribedPairs {
		mexcPair := strings.ToUpper(cp.Join("_"))
		if tickerResp.Symbol[mexcPair].LastPrice != 0 {
			p.setTickerPair(
				mexcPair,
				tickerResp.Symbol[mexcPair],
			)
			telemetryWebsocketMessage(ProviderMexc, MessageTypeTicker)
			return
		}
	}

	if tickerErr != nil {
		p.logger.Error().
			Int("length", len(bz)).
			AnErr("ticker", tickerErr).
			Msg("mexc: Error on receive message")
	}
}

func (p *MexcProvider) setTickerPair(symbol string, ticker MexcTicker) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.tickers[symbol] = types.TickerPrice{
		Price:  floatToDec(ticker.LastPrice),
		Volume: floatToDec(ticker.Volume),
		Time:   time.Now().UnixMilli(),
	}
}

func (p *MexcProvider) SetSubscribedPair(cp types.CurrencyPair) {
	p.subscribedPairs[cp.String()] = cp
}

// GetAvailablePairs returns all pairs to which the provider can subscribe.
// ex.: map["ATOMUSDT" => {}, "UMEEUSDC" => {}].
func (p *MexcProvider) GetAvailablePairs() (map[string]struct{}, error) {
	resp, err := http.Get(p.endpoints.Rest + mexcRestPath)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pairsSummary []MexcPairSummary
	if err := json.NewDecoder(resp.Body).Decode(&pairsSummary); err != nil {
		return nil, err
	}

	availablePairs := make(map[string]struct{}, len(pairsSummary))
	for _, pairName := range pairsSummary {
		availablePairs[strings.ToUpper(pairName.Symbol)] = struct{}{}
	}

	return availablePairs, nil
}
