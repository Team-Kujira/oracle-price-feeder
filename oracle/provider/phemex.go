package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"price-feeder/oracle/types"
	"sync"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	phemexWSHost   = "phemex.com"
	phemexWSPath   = "/ws"
	phemexRestHost = "https://api.phemex.com"
)

var _ Provider = (*PhemexProvider)(nil)

type (
	PhemexProvider struct {
		wsc             *WebsocketController
		logger          zerolog.Logger
		mtx             sync.Mutex
		endpoints       Endpoint
		tickers         map[string]PhemexTicker
		subscribedPairs map[string]types.CurrencyPair
	}

	PhemexTicker struct {
		Price  int64
		Volume int64
		Time   int64
	}

	PhemexWsSubscriptionMsg struct {
		ID     uint     `json:"id"`
		Method string   `json:"method"`
		Params []string `json:"params"`
	}

	PhemexGenericMsg struct {
		Error *PhemexWsError `json:"error"`
		ID    uint           `json:"id"`
	}

	PhemexWsError struct {
		Code    int64  `json:"code"`
		Message string `json:"message"`
	}

	PhemexWsTradeMsg struct {
		Type   string               `json:"type"`
		Symbol string               `json:"symbol"`
		Trades [][4]json.RawMessage `json:"trades"`
	}

	PhemexRestTickerResponse struct {
		Result PhemexRestTicker `json:"result"`
	}

	PhemexRestTicker struct {
		Price  int64 `json:"lastEp"`
		Volume int64 `json:"volumeEv"`
		Time   int64 `json:"timestamp"`
	}
)

func NewPhemexProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*PhemexProvider, error) {
	if endpoints.Name != ProviderPhemex {
		endpoints = Endpoint{
			Name:      ProviderPhemex,
			Rest:      phemexRestHost,
			Websocket: phemexWSHost,
		}
	}

	wsURL := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   phemexWSPath,
	}

	phemexLogger := logger.With().Str("provider", string(ProviderPhemex)).Logger()

	provider := &PhemexProvider{
		logger:          phemexLogger,
		endpoints:       endpoints,
		tickers:         map[string]PhemexTicker{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	setSubscribedPairs(provider, pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderPhemex,
		wsURL,
		provider.GetSubscriptionMsgs(pairs...),
		provider.messageReceived,
		defaultPingDuration,
		websocket.TextMessage,
		phemexLogger,
	)

	provider.wsc.pingMessage = `{"id":0,"method":"server.ping","params":[]}`

	go provider.wsc.Start()

	return provider, nil
}

func (p *PhemexProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, len(cps))

	for i, cp := range cps {
		subscriptionMsgs[i] = PhemexWsSubscriptionMsg{
			ID:     1,
			Method: "trade.subscribe",
			Params: []string{"s" + cp.String()},
		}
	}

	return subscriptionMsgs
}

func (p *PhemexProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	cp, ok := p.subscribedPairs[s]
	return cp, ok
}

func (p *PhemexProvider) SetSubscribedPair(cp types.CurrencyPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.subscribedPairs[cp.String()] = cp
}

func (p *PhemexProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	return subscribeCurrencyPairs(p, cps)
}

func (p *PhemexProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return p.wsc.AddSubscriptionMsgs(msgs)
}

// GetTickerPrices converts and returns all internally stored tickers and
// updates the volume data of all tickers. The volume data is retrieved via
// the REST endpoint which is takes some time. Therefore the update is run in
// parallel and the update takes effect after the current values are returned
func (p *PhemexProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	go func(p *PhemexProvider) {
		for _, cp := range cps {
			client := &http.Client{}

			req, err := http.NewRequest(
				"GET", p.endpoints.Rest+"/md/spot/ticker/24hr", nil,
			)
			if err != nil {
				p.logger.Err(err)
				continue
			}

			query := req.URL.Query()
			query.Add("symbol", "s"+cp.String())
			req.URL.RawQuery = query.Encode()

			resp, err := client.Do(req)
			if err != nil {
				p.logger.Err(err)
				continue
			}
			defer resp.Body.Close()

			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				p.logger.Err(err)
				continue
			}

			var tickerResponse PhemexRestTickerResponse

			err = json.Unmarshal(body, &tickerResponse)
			if err != nil {
				p.logger.Err(err)
				continue
			}

			p.setTickerVolume("s"+cp.String(), tickerResponse.Result.Volume)
		}

		p.logger.Info().Msg("Done updating volumes")
	}(p)

	return getTickerPrices(p, cps)
}

func (p *PhemexProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	key := "s" + cp.String()

	ticker, ok := p.tickers[key]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf("phemex failed to get ticker price for %s", key)
	}

	return types.TickerPrice{
		Price:  sdk.NewDec(ticker.Price).QuoInt64(1e8),
		Volume: sdk.NewDec(ticker.Volume).QuoInt64(1e8),
		Time:   ticker.Time / 1000,
	}, nil
}

func (p *PhemexProvider) messageReceived(messageType int, bz []byte) {
	var (
		tradeMsg   PhemexWsTradeMsg
		genericMsg PhemexGenericMsg
		tradeErr   error
	)

	tradeErr = json.Unmarshal(bz, &tradeMsg)
	if tradeErr == nil && len(tradeMsg.Trades) > 0 && tradeMsg.Type == "incremental" {
		p.setTickerPrice(tradeMsg)
		telemetryWebsocketMessage(ProviderPhemex, MessageTypeTrade)
		return
	}

	err := json.Unmarshal(bz, &genericMsg)
	if err == nil && genericMsg.Error == nil {
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("trade", tradeErr).
		Str("msg", string(bz)).
		Msg("Error on receive message")
}

func (p *PhemexProvider) setTickerVolume(symbol string, volume int64) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if ticker, ok := p.tickers[symbol]; ok {
		ticker.Volume = volume
		p.tickers[symbol] = ticker
	}
}

// setTickerPrice updates the price and time value of the ticker provided.
// Sets it with volume = 0, if not present in internal tickers map
func (p *PhemexProvider) setTickerPrice(tradeMsg PhemexWsTradeMsg) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	var price int64
	err := json.Unmarshal(tradeMsg.Trades[len(tradeMsg.Trades)-1][2], &price)
	if err != nil {
		return
	}

	time := time.Now().UnixMilli()

	if ticker, ok := p.tickers[tradeMsg.Symbol]; ok {
		ticker.Price = price
		ticker.Time = time
		p.tickers[tradeMsg.Symbol] = ticker
	} else {
		p.tickers[tradeMsg.Symbol] = PhemexTicker{
			Price:  price,
			Volume: 0,
			Time:   time,
		}
	}
}

func (p *PhemexProvider) GetAvailablePairs() (map[string]struct{}, error) {
	// not used yet, so skipping this unless needed
	return make(map[string]struct{}, 0), nil
}
