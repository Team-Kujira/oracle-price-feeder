package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	mexcWSHost   = "wbs.mexc.com"
	mexcWSPath   = "/ws"
	mexcRestHost = "https://api.mexc.com"
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
		tickers         map[string]MexcTicker         // Symbol => TickerPrice
		subscribedPairs map[string]types.CurrencyPair // Symbol => types.CurrencyPair
	}

	MexcWsSubscriptionMsg struct {
		Method string   `json:"method"`
		Params []string `json:"params"`
	}

	MexcWsCandleMsg struct {
		Symbol string           `json:"s"`
		Data   MexcWsCandleData `json:"d"`
		Time   int64            `json:"t"`
	}

	MexcWsCandleData struct {
		Candle MexcWsCandle `json:"k"`
	}

	MexcWsCandle struct {
		Close float64 `json:"c"`
	}

	MexcRestTickerResponse struct {
		Symbol string `json:"symbol"`
		Volume string `json:"volume"`
	}

	MexcTicker struct {
		Price  float64
		Volume string
		Time   int64
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
		tickers:         map[string]MexcTicker{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	setSubscribedPairs(provider, pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderMexc,
		wsURL,
		provider.GetSubscriptionMsgs(pairs...),
		provider.messageReceived,
		defaultPingDuration,
		websocket.PingMessage,
		mexcLogger,
	)
	go provider.wsc.Start()

	return provider, nil
}

func (p *MexcProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, 1)

	params := make([]string, len(cps))

	for i, cp := range cps {
		params[i] = "spot@public.kline.v3.api@" + cp.String() + "@Min15"
	}

	subscriptionMsgs[0] = MexcWsSubscriptionMsg{
		Method: "SUBSCRIPTION",
		Params: params,
	}

	return subscriptionMsgs
}

func (p *MexcProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	cp, ok := p.subscribedPairs[s]
	return cp, ok
}

func (p *MexcProvider) SetSubscribedPair(cp types.CurrencyPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.subscribedPairs[cp.String()] = cp
}

// SubscribeCurrencyPairs sends the new subscription messages to the websocket
// and adds them to the providers subscribedPairs array
func (p *MexcProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	return subscribeCurrencyPairs(p, cps)
}

func (p *MexcProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return p.wsc.AddSubscriptionMsgs(msgs)
}

// GetTickerPrices returns the tickerPrices based on the provided pairs.
func (p *MexcProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	go func() {
		for _, cp := range cps {
			client := &http.Client{}

			req, err := http.NewRequest(
				"GET", p.endpoints.Rest+"/api/v3/ticker/24hr", nil,
			)
			if err != nil {
				p.logger.Err(err)
				continue
			}

			query := req.URL.Query()
			query.Add("symbol", cp.String())
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

			var tickerResponse MexcRestTickerResponse

			err = json.Unmarshal(body, &tickerResponse)
			if err != nil {
				p.logger.Err(err)
				continue
			}

			p.setTickerVolume(tickerResponse.Symbol, tickerResponse.Volume)
		}

		p.logger.Info().Msg("Done updating volumes")
	}()

	return getTickerPrices(p, cps)
}

func (p *MexcProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	key := cp.String()

	ticker, ok := p.tickers[key]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf(
			types.ErrTickerNotFound.Error(),
			ProviderMexc,
			key,
		)
	}

	return types.TickerPrice{
		Price:  sdk.MustNewDecFromStr(fmt.Sprintf("%f", ticker.Price)),
		Volume: sdk.MustNewDecFromStr(ticker.Volume),
		Time:   ticker.Time,
	}, nil
}

func (p *MexcProvider) messageReceived(messageType int, bz []byte) {
	var (
		candleMsg MexcWsCandleMsg
		candleErr error
	)

	candleErr = json.Unmarshal(bz, &candleMsg)
	if candleErr == nil && candleMsg.Symbol != "" {
		p.setTickerPrice(candleMsg)
		telemetryWebsocketMessage(ProviderMexc, MessageTypeTicker)
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("candle", candleErr).
		Str("msg", string(bz)).
		Msg("mexc: Error on receive message")
}

func (p *MexcProvider) setTickerVolume(symbol string, volume string) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if ticker, ok := p.tickers[symbol]; ok {
		ticker.Volume = volume
		p.tickers[symbol] = ticker
	}
}

func (p *MexcProvider) setTickerPrice(candleMsg MexcWsCandleMsg) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if ticker, ok := p.tickers[candleMsg.Symbol]; ok {
		ticker.Price = candleMsg.Data.Candle.Close
		ticker.Time = candleMsg.Time
		p.tickers[candleMsg.Symbol] = ticker
	} else {
		p.tickers[candleMsg.Symbol] = MexcTicker{
			Price:  candleMsg.Data.Candle.Close,
			Volume: "0",
			Time:   candleMsg.Time,
		}
	}
}

// GetAvailablePairs returns all pairs to which the provider can subscribe.
// ex.: map["ATOMUSDT" => {}, "UMEEUSDC" => {}].
func (p *MexcProvider) GetAvailablePairs() (map[string]struct{}, error) {
	// not used yet, so skipping this unless needed
	return make(map[string]struct{}, 0), nil
}
