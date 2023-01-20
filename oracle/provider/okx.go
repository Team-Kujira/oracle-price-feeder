package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
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
		tickers         map[string]OkxTicker          // InstId => OkxTickerPair
		subscribedPairs map[string]types.CurrencyPair // Symbol => types.CurrencyPair
	}

	// OkxSubscriptionMsg Message to subscribe/unsubscribe with N Topics.
	OkxWsSubscriptionMsg struct {
		Op   string                   `json:"op"` // Operation ex.: subscribe
		Args []OkxWsSubscriptionTopic `json:"args"`
	}

	// OkxSubscriptionTopic Topic with the ticker to be subscribed/unsubscribed.
	OkxWsSubscriptionTopic struct {
		Channel string `json:"channel"` // Channel name ex.: tickers
		InstID  string `json:"instId"`  // Instrument ID ex.: BTC-USDT
	}

	OkxWsGenericMsg struct {
		Event string `json:"event"`
	}

	OkxWsCandleMsg struct {
		Args OkxWsSubscriptionTopic `json:"arg"`
		Data [][9]string            `json:"data"`
	}

	OkxRestTickerResponse struct {
		Data []OkxRestTicker `json:"data"`
	}

	OkxRestTicker struct {
		InstID string `json:"instId"`
		Volume string `json:"vol24h"`
	}

	OkxTicker struct {
		Price  string
		Volume string
		Time   int64
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
		tickers:         map[string]OkxTicker{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	setSubscribedPairs(provider, pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderOkx,
		wsURL,
		provider.GetSubscriptionMsgs(pairs...),
		provider.messageReceived,
		defaultPingDuration,
		websocket.PingMessage,
		okxLogger,
	)
	go provider.wsc.Start()

	return provider, nil
}

func (p *OkxProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, 1)

	okxTopics := make([]OkxWsSubscriptionTopic, len(cps))

	for i, cp := range cps {
		okxTopics[i] = OkxWsSubscriptionTopic{
			Channel: "candle3m",
			InstID:  cp.Join("-"),
		}
	}

	subscriptionMsgs[0] = OkxWsSubscriptionMsg{
		Op:   "subscribe",
		Args: okxTopics,
	}

	return subscriptionMsgs
}

func (p *OkxProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	cp, ok := p.subscribedPairs[s]
	return cp, ok
}

func (p *OkxProvider) SetSubscribedPair(cp types.CurrencyPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.subscribedPairs[cp.String()] = cp
}

func (p *OkxProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return subscribeCurrencyPairs(p, cps)
}

func (p *OkxProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return p.wsc.AddSubscriptionMsgs(msgs)
}

// GetTickerPrices returns the tickerPrices based on the saved map.
func (p *OkxProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {

	go func(p *OkxProvider) {
		requiredSymbols := make(map[string]bool, len(cps))

		for _, cp := range cps {
			requiredSymbols[cp.Join("-")] = true
		}

		client := &http.Client{}

		req, err := http.NewRequest(
			"GET", p.endpoints.Rest+"/api/v5/market/tickers", nil,
		)
		if err != nil {
			p.logger.Err(err)
		}

		query := req.URL.Query()
		query.Add("instType", "SWAP")
		req.URL.RawQuery = query.Encode()

		resp, err := client.Do(req)
		if err != nil {
			p.logger.Err(err)
		}
		defer resp.Body.Close()

		var tickerResp OkxRestTickerResponse
		err = json.NewDecoder(resp.Body).Decode(&tickerResp)
		if err != nil {
			return
		}

		for _, ticker := range tickerResp.Data {
			if _, ok := requiredSymbols[ticker.InstID]; ok {
				p.setTickerVolume(ticker.InstID, ticker.Volume)
			}
		}

		p.logger.Info().Msg("Done updating volumes")

	}(p)

	return getTickerPrices(p, cps)
}

func (p *OkxProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	instrumentID := cp.Join("-")
	ticker, ok := p.tickers[instrumentID]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf("okx failed to get ticker price for %s", instrumentID)
	}

	return types.TickerPrice{
		Price:  sdk.MustNewDecFromStr(ticker.Price),
		Volume: sdk.MustNewDecFromStr(ticker.Volume),
		Time:   ticker.Time,
	}, nil
}

func (p *OkxProvider) messageReceived(messageType int, bz []byte) {
	var (
		candleMsg  OkxWsCandleMsg
		genericMsg OkxWsGenericMsg
	)

	// sometimes the message received is not a ticker or a candle response.
	err := json.Unmarshal(bz, &candleMsg)
	if err == nil && len(candleMsg.Data) > 0 {
		p.setTicker(candleMsg)
		telemetryWebsocketMessage(ProviderOkx, MessageTypeTicker)
		return
	}

	err = json.Unmarshal(bz, &genericMsg)
	if err == nil && genericMsg.Event == "subscribe" {
		// subscription response
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("ticker", err).
		Str("msg", string(bz)).
		Msg("Error on receive message")
}

func (p *OkxProvider) setTicker(candleMsg OkxWsCandleMsg) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	symbol := candleMsg.Args.InstID
	price := candleMsg.Data[0][4]
	timestamp := time.Now().UnixMilli()

	if ticker, ok := p.tickers[symbol]; ok {
		ticker.Price = price
		ticker.Time = timestamp
		p.tickers[symbol] = ticker
	} else {
		p.tickers[symbol] = OkxTicker{
			Price:  price,
			Volume: "0",
			Time:   timestamp,
		}
	}
}

func (p *OkxProvider) setTickerVolume(symbol string, volume string) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if ticker, ok := p.tickers[symbol]; ok {
		ticker.Volume = volume
		p.tickers[symbol] = ticker
	}
}

// GetAvailablePairs return all available pairs symbol to subscribe.
func (p *OkxProvider) GetAvailablePairs() (map[string]struct{}, error) {
	// not used yet, so skipping this unless needed
	return make(map[string]struct{}, 0), nil
}
