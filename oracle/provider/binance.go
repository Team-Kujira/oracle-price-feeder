package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	binanceWSHost     = "stream.binance.com:9443"
	binanceUSWSHost   = "stream.binance.us:9443"
	binanceWSPath     = "/ws/umeestream"
	binanceRestHost   = "https://api1.binance.com"
	binanceRestUSHost = "https://api.binance.us"
	binanceRestPath   = "/api/v3/ticker/price"
)

var _ Provider = (*BinanceProvider)(nil)

type (
	// BinanceProvider defines an Oracle provider implemented by the Binance public
	// API.
	//
	// REF: https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-mini-ticker-stream
	// REF: https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-streams
	BinanceProvider struct {
		wsc             *WebsocketController
		logger          zerolog.Logger
		mtx             sync.RWMutex
		endpoints       Endpoint
		tickers         map[string]BinanceTicker      // Symbol => BinanceTicker
		subscribedPairs map[string]types.CurrencyPair // Symbol => types.CurrencyPair
	}

	// BinanceSubscribeMsg Msg to subscribe all the tickers channels.
	BinanceWsSubscriptionMsg struct {
		Method string   `json:"method"` // SUBSCRIBE/UNSUBSCRIBE
		Params []string `json:"params"` // streams to subscribe ex.: usdtatom@ticker
		ID     uint16   `json:"id"`     // identify messages going back and forth
	}

	// BinanceWsSubscriptionResponse the response structure for a binance subscription response
	BinanceWsSubscriptionResponse struct {
		Result string `json:"result"`
		ID     uint16 `json:"id"`
	}

	BinanceWsCandleMsg struct {
		EventType string          `json:"e"`
		Symbol    string          `json:"s"`
		Time      int64           `json:"E"`
		Candle    BinanceWsCandle `json:"k"`
	}

	BinanceWsCandle struct {
		Close string `json:"c"`
	}

	BinanceRest24hTicker struct {
		Symbol string `json:"symbol"`
		Volume string `json:"volume"`
	}

	BinanceTicker struct {
		Price  string
		Volume string
		Time   int64
	}
)

func NewBinanceProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	binanceUS bool,
	pairs ...types.CurrencyPair,
) (*BinanceProvider, error) {
	if (endpoints.Name) != ProviderBinance {
		if !binanceUS {
			endpoints = Endpoint{
				Name:      ProviderBinance,
				Rest:      binanceRestHost,
				Websocket: binanceWSHost,
			}
		} else {
			endpoints = Endpoint{
				Name:      ProviderBinanceUS,
				Rest:      binanceRestUSHost,
				Websocket: binanceUSWSHost,
			}
		}
	}

	wsURL := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   binanceWSPath,
	}

	binanceLogger := logger.With().Str("provider", string(ProviderBinance)).Logger()

	provider := &BinanceProvider{
		logger:          binanceLogger,
		endpoints:       endpoints,
		tickers:         map[string]BinanceTicker{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	setSubscribedPairs(provider, pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderBinance,
		wsURL,
		provider.GetSubscriptionMsgs(pairs...),
		provider.messageReceived,
		disabledPingDuration,
		websocket.PingMessage,
		binanceLogger,
	)
	go provider.wsc.Start()

	return provider, nil
}

func (p *BinanceProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	msg := BinanceWsSubscriptionMsg{
		Method: "SUBSCRIBE",
		Params: make([]string, len(cps)),
		ID:     1,
	}
	for i, cp := range cps {
		msg.Params[i] = strings.ToLower(cp.String()) + "@kline_1m"
	}
	return []interface{}{msg}
}

func (p *BinanceProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	cp, ok := p.subscribedPairs[s]
	return cp, ok
}

func (p *BinanceProvider) SetSubscribedPair(cp types.CurrencyPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.subscribedPairs[cp.String()] = cp
}

func (p *BinanceProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	return subscribeCurrencyPairs(p, cps)
}

func (p *BinanceProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return p.wsc.AddSubscriptionMsgs(msgs)
}

// GetTickerPrices returns the tickerPrices based on the provided pairs.
func (p *BinanceProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	go func(p *BinanceProvider) {
		requiredSymbols := make(map[string]bool, len(cps))

		for _, cp := range cps {
			requiredSymbols[cp.String()] = true
		}

		resp, err := http.Get(p.endpoints.Rest + "/api/v3/ticker/24hr")
		if err != nil {
			return
		}
		defer resp.Body.Close()

		var tickerResp []BinanceRest24hTicker
		err = json.NewDecoder(resp.Body).Decode(&tickerResp)
		if err != nil {
			return
		}

		for _, ticker := range tickerResp {
			if _, ok := requiredSymbols[ticker.Symbol]; ok {
				p.setTickerVolume(ticker.Symbol, ticker.Volume)
			}
		}

		p.logger.Info().Msg("Done updating volumes")

	}(p)

	return getTickerPrices(p, cps)
}

func (p *BinanceProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	key := cp.String()

	ticker, ok := p.tickers[key]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf("binance failed to get ticker price for %s", key)
	}

	return types.TickerPrice{
		Price:  sdk.MustNewDecFromStr(ticker.Price),
		Volume: sdk.MustNewDecFromStr(ticker.Volume),
		Time:   ticker.Time * 1000,
	}, nil
}

func (p *BinanceProvider) messageReceived(messageType int, bz []byte) {
	var (
		// tickerResp       BinanceTicker
		// tickerErr        error
		candleMsg        BinanceWsCandleMsg
		candleErr        error
		subscribeResp    BinanceWsSubscriptionResponse
		subscribeRespErr error
	)

	// tickerErr = json.Unmarshal(bz, &tickerResp)
	// if tickerErr == nil {
	// 	p.setTickerPair(tickerResp)
	// 	telemetryWebsocketMessage(ProviderBinance, MessageTypeTicker)
	// 	return
	// }

	candleErr = json.Unmarshal(bz, &candleMsg)
	if candleErr == nil {
		p.setTickerPrice(candleMsg)
		telemetryWebsocketMessage(ProviderBinance, MessageTypeCandle)
		return
	}

	subscribeRespErr = json.Unmarshal(bz, &subscribeResp)
	if subscribeResp.ID == 1 {
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("candle", candleErr).
		AnErr("subscribeResp", subscribeRespErr).
		Str("msg", string(bz)).
		Msg("Error on receive message")
}

// func (p *BinanceProvider) setTickerPair(ticker BinanceTicker) {
// 	p.mtx.Lock()
// 	defer p.mtx.Unlock()
// 	p.tickers[ticker.Symbol] = ticker
// }

func (p *BinanceProvider) setTickerPrice(candleMsg BinanceWsCandleMsg) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	symbol := candleMsg.Symbol

	if ticker, ok := p.tickers[symbol]; ok {
		ticker.Price = candleMsg.Candle.Close
		ticker.Time = candleMsg.Time
		p.tickers[symbol] = ticker
	} else {
		p.tickers[symbol] = BinanceTicker{
			Price:  candleMsg.Candle.Close,
			Volume: "0",
			Time:   candleMsg.Time,
		}
	}
}

func (p *BinanceProvider) setTickerVolume(symbol string, volume string) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if ticker, ok := p.tickers[symbol]; ok {
		ticker.Volume = volume
		p.tickers[symbol] = ticker
	}
}

// GetAvailablePairs returns all pairs to which the provider can subscribe.
// ex.: map["ATOMUSDT" => {}, "UMEEUSDC" => {}].
func (p *BinanceProvider) GetAvailablePairs() (map[string]struct{}, error) {
	// not used yet, so skipping this unless needed
	return make(map[string]struct{}, 0), nil
}

// func (ticker BinanceTicker) toTickerPrice() (types.TickerPrice, error) {
// 	tickerPrice, err := types.NewTickerPrice(
// 		string(ProviderBinance),
// 		ticker.Symbol,
// 		ticker.LastPrice,
// 		ticker.Volume,
// 		int64(ticker.Time),
// 	)
// 	if err != nil {
// 		return types.TickerPrice{}, err
// 	}
// 	return tickerPrice, nil
// }
