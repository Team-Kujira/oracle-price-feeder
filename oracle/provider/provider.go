package provider

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

const (
	defaultTimeout       = 10 * time.Second
	staleTickersCutoff   = 1 * time.Minute
	providerCandlePeriod = 10 * time.Minute

	ProviderFin       Name = "fin"
	ProviderKraken    Name = "kraken"
	ProviderBinance   Name = "binance"
	ProviderBinanceUS Name = "binanceus"
	ProviderOsmosis   Name = "osmosis"
	ProviderOsmosisV2 Name = "osmosisv2"
	ProviderHuobi     Name = "huobi"
	ProviderOkx       Name = "okx"
	ProviderGate      Name = "gate"
	ProviderCoinbase  Name = "coinbase"
	ProviderBitget    Name = "bitget"
	ProviderBitfinex  Name = "bitfinex"
	ProviderBitforex  Name = "bitforex"
	ProviderHitbtc    Name = "hitbtc"
	ProviderPoloniex  Name = "poloniex"
	ProviderPhemex    Name = "phemex"
	ProviderLbank     Name = "lbank"
	ProviderKucoin    Name = "kucoin"
	ProviderBybit     Name = "bybit"
	ProviderMexc      Name = "mexc"
	ProviderCrypto    Name = "crypto"
	ProviderMock      Name = "mock"
	ProviderStride    Name = "stride"
)

type (
	// Provider defines an interface an exchange price provider must implement.
	Provider interface {
		// GetTickerPrices returns the tickerPrices based on the provided pairs.
		GetTickerPrices(...types.CurrencyPair) (map[string]types.TickerPrice, error)
		// SubscribeCurrencyPairs sends subscription messages for the new currency
		// pairs and adds them to the providers subscribed pairs
		SubscribeCurrencyPairs(...types.CurrencyPair) error
		CurrencyPairToProviderPair(types.CurrencyPair) string
		ProviderPairToCurrencyPair(string) types.CurrencyPair
	}

	provider struct {
		ctx       context.Context
		endpoints Endpoint
		http      *http.Client
		logger    zerolog.Logger
		mtx       sync.RWMutex
		pairs     map[string]types.CurrencyPair
		tickers   map[string]types.TickerPrice
		websocket *WebsocketController
	}

	PollingProvider interface {
		Poll() error
	}

	// Name name of an oracle provider. Usually it is an exchange
	// but this can be any provider name that can give token prices
	// examples.: "binance", "osmosis", "kraken".
	Name string

	// AggregatedProviderPrices defines a type alias for a map
	// of provider -> asset -> TickerPrice
	AggregatedProviderPrices map[Name]map[string]types.TickerPrice

	// Endpoint defines an override setting in our config for the
	// hardcoded rest and websocket api endpoints.
	Endpoint struct {
		Name          Name   // ex. "binance"
		Rest          string // ex. "https://api1.binance.com"
		Websocket     string // ex. "stream.binance.com:9443"
		WebsocketPath string
		PollInterval  time.Duration
		PingDuration  time.Duration
		PingType      uint
		PingMessage   string
	}
)

func (p *provider) Init(
	ctx context.Context,
	endpoints Endpoint,
	logger zerolog.Logger,
	pairs []types.CurrencyPair,
	websocketMessageHandler MessageHandler,
	websocketSubscribeHandler SubscribeHandler,
) {
	p.ctx = ctx
	p.endpoints = endpoints
	p.endpoints.SetDefaults()
	p.logger = logger.With().Str("provider", p.endpoints.Name.String()).Logger()
	p.pairs = make(map[string]types.CurrencyPair, len(pairs))
	for _, pair := range pairs {
		p.pairs[pair.String()] = pair
	}
	p.tickers = make(map[string]types.TickerPrice, len(pairs))
	p.http = newDefaultHTTPClient()
	if p.endpoints.Websocket != "" {
		websocketUrl := url.URL{
			Scheme: "wss",
			Host:   p.endpoints.Websocket,
			Path:   p.endpoints.WebsocketPath,
		}
		p.websocket = NewWebsocketController(
			ctx,
			p.endpoints.Name,
			websocketUrl,
			pairs,
			websocketMessageHandler,
			websocketSubscribeHandler,
			p.endpoints.PingDuration,
			p.endpoints.PingType,
			p.endpoints.PingMessage,
			p.logger,
		)
		go p.websocket.Start()
	}
}

func (p *provider) GetTickerPrices(pairs ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	tickers := make(map[string]types.TickerPrice, len(pairs))
	for _, pair := range pairs {
		symbol := pair.String()
		price, ok := p.tickers[symbol]
		if !ok {
			p.logger.Warn().Str("pair", symbol).Msg("missing ticker price for pair")
		} else {
			if time.Since(price.Time) > staleTickersCutoff {
				p.logger.Warn().Str("pair", symbol).Time("time", price.Time).Msg("tickers data is stale")
			} else {
				tickers[symbol] = price
			}
		}
	}
	return tickers, nil
}

func (p *provider) SubscribeCurrencyPairs(pairs ...types.CurrencyPair) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	newPairs := p.addPairs(pairs...)
	if p.endpoints.Websocket == "" {
		return nil
	}
	return p.websocket.AddPairs(newPairs)
}

func (p *provider) addPairs(pairs ...types.CurrencyPair) []types.CurrencyPair {
	newPairs := []types.CurrencyPair{}
	for _, pair := range pairs {
		_, ok := p.pairs[pair.String()]
		if !ok {
			newPairs = append(newPairs, pair)
		}
	}
	return newPairs
}

func (p *provider) CurrencyPairToProviderPair(pair types.CurrencyPair) string {
	return pair.Base + "_" + pair.Quote
}

func (p *provider) ProviderPairToCurrencyPair(pair string) types.CurrencyPair {
	tokens := strings.Split(pair, "_")
	if len(tokens) != 2 {
		p.logger.Warn().Str("pair", pair).Msg("failed to convert to currency pair")
		return types.CurrencyPair{}
	}
	return types.CurrencyPair{
		Base:  tokens[0],
		Quote: tokens[1],
	}
}

func (p *provider) makeHttpRequest(url string) ([]byte, error) {
	resp, err := p.http.Get(url)
	if err != nil {
		p.logger.Warn().
			Err(err).
			Msg("failed requesting tickers")
		return nil, err
	}
	if resp.StatusCode != 200 {
		p.logger.Warn().
			Int("code", resp.StatusCode).
			Msg("request returned invalid status")
		if resp.StatusCode == 429 || resp.StatusCode == 418 {
			backoffSeconds, err := strconv.Atoi(resp.Header.Get("Retry-After"))
			if err != nil {
				return nil, err
			}
			p.logger.Warn().
				Int("seconds", backoffSeconds).
				Msg("ratelimit backoff")
			time.Sleep(time.Duration(backoffSeconds) * time.Second)
			return nil, nil
		}
	}
	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return content, nil
}

func (e *Endpoint) SetDefaults() {
	var defaults Endpoint
	switch e.Name {
	case ProviderBinance:
		defaults = binanceDefaultEndpoints
	case ProviderBinanceUS:
		defaults = binanceUSDefaultEndpoints
	case ProviderBybit:
		defaults = bybitDefaultEndpoints
	case ProviderCrypto:
		defaults = cryptoDefaultEndpoints
	case ProviderFin:
		defaults = finDefaultEndpoints
	case ProviderGate:
		defaults = gateDefaultEndpoints
	case ProviderHuobi:
		defaults = huobiDefaultEndpoints
	case ProviderKucoin:
		defaults = kucoinDefaultEndpoints
	case ProviderMexc:
		defaults = mexcDefaultEndpoints
	case ProviderMock:
		defaults = mockDefaultEndpoints
	case ProviderOkx:
		defaults = okxDefaultEndpoints
	case ProviderOsmosis:
		defaults = osmosisDefaultEndpoints
	case ProviderOsmosisV2:
		defaults = osmosisv2DefaultEndpoints
	default:
		return
	}
	if e.Rest == "" {
		e.Rest = defaults.Rest
	}
	if e.Websocket == "" && defaults.Websocket != "" { // don't enable websockets for providers that don't support them
		e.Websocket = defaults.Websocket
	}
	if e.WebsocketPath == "" {
		e.WebsocketPath = defaults.WebsocketPath
	}
	if e.PollInterval == time.Duration(0) {
		e.PollInterval = defaults.PollInterval
	}
	if e.PingDuration == time.Duration(0) {
		e.PingDuration = defaults.PingDuration
	}
	if e.PingType == 0 {
		e.PingType = defaults.PingType
	}
	if e.PingMessage == "" {
		if defaults.PingMessage != "" {
			e.PingMessage = defaults.PingMessage
		} else {
			e.PingMessage = "ping"
		}
	}
}

func startPolling(p PollingProvider, interval time.Duration, logger zerolog.Logger) {
	logger.Debug().Dur("interval", interval).Msg("starting poll loop")
	for {
		err := p.Poll()
		if err != nil {
			logger.Error().Err(err).Msg("failed to poll")
		}
		time.Sleep(interval)
	}
}

func (p *provider) GetAvailablePairs() (map[string]struct{}, error) {
	p.logger.Warn().Msg("available pairs query not implemented")
	return map[string]struct{}{}, nil
}

// String cast provider name to string.
func (n Name) String() string {
	return string(n)
}

// preventRedirect avoid any redirect in the http.Client the request call
// will not return an error, but a valid response with redirect response code.
func preventRedirect(_ *http.Request, _ []*http.Request) error {
	return http.ErrUseLastResponse
}

func newDefaultHTTPClient() *http.Client {
	return newHTTPClientWithTimeout(defaultTimeout)
}

func newHTTPClientWithTimeout(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:       timeout,
		CheckRedirect: preventRedirect,
	}
}

// PastUnixTime returns a millisecond timestamp that represents the unix time
// minus t.
func PastUnixTime(t time.Duration) int64 {
	return time.Now().Add(t*-1).Unix() * int64(time.Second/time.Millisecond)
}

// SecondsToMilli converts seconds to milliseconds for our unix timestamps.
func SecondsToMilli(t int64) int64 {
	return t * int64(time.Second/time.Millisecond)
}

func checkHTTPStatus(resp *http.Response) error {
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
	return nil
}

func strToDec(str string) sdk.Dec {
	if strings.Contains(str, ".") {
		split := strings.Split(str, ".")
		if len(split[1]) > 18 {
			// sdk.MustNewDecFromStr will panic if decimal precision is greater than 18
			str = split[0] + "." + split[1][0:18]
		}
	}
	return sdk.MustNewDecFromStr(str)
}

func floatToDec(f float64) sdk.Dec {
	return sdk.MustNewDecFromStr(strconv.FormatFloat(f, 'f', -1, 64))
}
