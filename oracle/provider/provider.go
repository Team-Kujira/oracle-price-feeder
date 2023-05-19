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

	ProviderFin        Name = "fin"
	ProviderFinUsk     Name = "finusk"
	ProviderKraken     Name = "kraken"
	ProviderBinance    Name = "binance"
	ProviderBinanceUS  Name = "binanceus"
	ProviderOsmosis    Name = "osmosis"
	ProviderOsmosisV2  Name = "osmosisv2"
	ProviderHuobi      Name = "huobi"
	ProviderOkx        Name = "okx"
	ProviderGate       Name = "gate"
	ProviderCoinbase   Name = "coinbase"
	ProviderBitget     Name = "bitget"
	ProviderBitmart    Name = "bitmart"
	ProviderBkex       Name = "bkex"
	ProviderBitfinex   Name = "bitfinex"
	ProviderBitforex   Name = "bitforex"
	ProviderHitBtc     Name = "hitbtc"
	ProviderPoloniex   Name = "poloniex"
	ProviderPyth       Name = "pyth"
	ProviderPhemex     Name = "phemex"
	ProviderLbank      Name = "lbank"
	ProviderKucoin     Name = "kucoin"
	ProviderBybit      Name = "bybit"
	ProviderMexc       Name = "mexc"
	ProviderCrypto     Name = "crypto"
	ProviderCurve      Name = "curve"
	ProviderMock       Name = "mock"
	ProviderStride     Name = "stride"
	ProviderXt         Name = "xt"
	ProviderIdxOsmosis Name = "idxosmosis"
	ProviderZero       Name = "zero"
)

type (
	// Provider defines an interface an exchange price provider must implement.
	Provider interface {
		// GetTickerPrices returns the tickerPrices based on the provided pairs.
		GetTickerPrices(...types.CurrencyPair) (map[string]types.TickerPrice, error)
		// GetAvailablePairs returns the list of all supported pairs.
		GetAvailablePairs() (map[string]struct{}, error)

		// SubscribeCurrencyPairs sends subscription messages for the new currency
		// pairs and adds them to the providers subscribed pairs
		SubscribeCurrencyPairs(...types.CurrencyPair) error
		CurrencyPairToProviderPair(types.CurrencyPair) string
		// ProviderPairToCurrencyPair(string) types.CurrencyPair
	}

	CurrencyPairToProviderSymbol func(types.CurrencyPair) string

	provider struct {
		ctx       context.Context
		endpoints Endpoint
		httpBase  string
		http      *http.Client
		logger    zerolog.Logger
		mtx       sync.RWMutex
		pairs     map[string]types.CurrencyPair
		inverse   map[string]types.CurrencyPair
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
		Name          Name // ex. "binance"
		Urls          []string
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
	p.tickers = map[string]types.TickerPrice{}
	p.http = newDefaultHTTPClient()
	p.httpBase = p.endpoints.Urls[0]
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
			if price.Price.IsZero() {
				p.logger.Warn().
					Str("pair", symbol).
					Msg("ticker price is '0'")
				continue
			}
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
	return pair.String()
}

func (p *provider) httpGet(path string) ([]byte, error) {
	res, err := p.makeHttpRequest(p.httpBase + path)
	if err != nil {
		p.logger.Warn().
			Str("endpoint", p.httpBase).
			Str("path", path).
			Msg("trying alternate http endpoints")
		for _, endpoint := range p.endpoints.Urls {
			if endpoint == p.httpBase {
				continue
			}
			res, err = p.makeHttpRequest(endpoint + path)
			if err == nil {
				p.logger.Info().Str("endpoint", endpoint).Msg("selected alternate http endpoint")
				p.httpBase = endpoint
				break
			}
		}
	}
	return res, err
}

func (p *provider) makeHttpRequest(url string) ([]byte, error) {
	res, err := p.http.Get(url)
	if err != nil {
		p.logger.Warn().
			Err(err).
			Msg("http request failed")
		return nil, err
	}
	if res.StatusCode != 200 {
		p.logger.Warn().
			Int("code", res.StatusCode).
			Msg("http request returned invalid status")
		if res.StatusCode == 429 || res.StatusCode == 418 {
			p.logger.Warn().
				Str("url", url).
				Str("retry_after", res.Header.Get("Retry-After")).
				Msg("http ratelimited")
		}
		return nil, fmt.Errorf("http request returned invalid status")
	}
	content, err := ioutil.ReadAll(res.Body)
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
	case ProviderBitfinex:
		defaults = bitfinexDefaultEndpoints
	case ProviderBinanceUS:
		defaults = binanceUSDefaultEndpoints
	case ProviderBitget:
		defaults = bitgetDefaultEndpoints
	case ProviderBitmart:
		defaults = bitmartDefaultEndpoints
	case ProviderBkex:
		defaults = bkexDefaultEndpoints
	case ProviderBybit:
		defaults = bybitDefaultEndpoints
	case ProviderCoinbase:
		defaults = coinbaseDefaultEndpoints
	case ProviderCrypto:
		defaults = cryptoDefaultEndpoints
	case ProviderCurve:
		defaults = curveDefaultEndpoints
	case ProviderFin:
		defaults = finDefaultEndpoints
	case ProviderFinUsk:
		defaults = finUskDefaultEndpoints
	case ProviderGate:
		defaults = gateDefaultEndpoints
	case ProviderHitBtc:
		defaults = hitbtcDefaultEndpoints
	case ProviderIdxOsmosis:
		defaults = idxOsmosisDefaultEndpoints
	case ProviderHuobi:
		defaults = huobiDefaultEndpoints
	case ProviderKraken:
		defaults = krakenDefaultEndpoints
	case ProviderKucoin:
		defaults = kucoinDefaultEndpoints
	case ProviderLbank:
		defaults = lbankDefaultEndpoints
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
	case ProviderPhemex:
		defaults = phemexDefaultEndpoints
	case ProviderPoloniex:
		defaults = poloniexDefaultEndpoints
	case ProviderPyth:
		defaults = pythDefaultEndpoints
	case ProviderXt:
		defaults = xtDefaultEndpoints
	case ProviderZero:
		defaults = zeroDefaultEndpoints
	default:
		return
	}
	if e.Urls == nil {
		e.Urls = defaults.Urls
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

func (p *provider) setPairs(
	pairs []types.CurrencyPair,
	availablePairs map[string]struct{},
	toProviderSymbol CurrencyPairToProviderSymbol,
) error {
	p.pairs = map[string]types.CurrencyPair{}
	p.inverse = map[string]types.CurrencyPair{}

	if toProviderSymbol == nil {
		toProviderSymbol = func(pair types.CurrencyPair) string {
			return pair.String()
		}
	}

	if availablePairs == nil {
		p.logger.Warn().Msg("available pairs not provided")

		for _, pair := range pairs {
			// If availablePairs is nil, GetAvailablePairs() is probably
			// not implemented for this provider
			inverted := pair.Swap()

			p.pairs[toProviderSymbol(pair)] = pair
			p.inverse[toProviderSymbol(inverted)] = pair
		}
		return nil
	}

	for _, pair := range pairs {
		inverted := pair.Swap()

		providerSymbol := toProviderSymbol(inverted)
		_, found := availablePairs[providerSymbol]
		if found {
			p.inverse[providerSymbol] = pair
			continue
		}

		symbol := pair.String()
		providerSymbol = toProviderSymbol(pair)
		_, found = availablePairs[providerSymbol]
		if found {
			p.pairs[providerSymbol] = pair
			continue
		}

		p.logger.Error().
			Msgf("%s is not supported by this provider", symbol)
	}

	return nil
}

func (p *provider) setTickerPrice(symbol string, price sdk.Dec, volume sdk.Dec, timestamp time.Time) {

	// check if price needs to be inverted
	pair, inverse := p.inverse[symbol]
	if inverse {
		price = invertDec(price)
		volume = volume.Mul(price)

		p.tickers[pair.String()] = types.TickerPrice{
			Price:  price,
			Volume: volume,
			Time:   timestamp,
		}

		TelemetryProviderPrice(
			p.endpoints.Name,
			pair.String(),
			float32(price.MustFloat64()),
			float32(volume.MustFloat64()),
		)

		return
	}

	pair, found := p.pairs[symbol]
	if !found {
		p.logger.Error().
			Str("symbol", symbol).
			Msg("symbol not found")
		return
	}

	p.tickers[pair.String()] = types.TickerPrice{
		Price:  price,
		Volume: volume,
		Time:   timestamp,
	}

	TelemetryProviderPrice(
		p.endpoints.Name,
		pair.String(),
		float32(price.MustFloat64()),
		float32(volume.MustFloat64()),
	)
}

func (p *provider) isPair(symbol string) bool {
	if _, found := p.pairs[symbol]; found {
		return true
	}

	if _, found := p.inverse[symbol]; found {
		return true
	}

	return false
}

func (p *provider) getAllPairs() map[string]types.CurrencyPair {
	pairs := map[string]types.CurrencyPair{}

	for symbol, pair := range p.pairs {
		pairs[symbol] = pair
	}

	for symbol, pair := range p.inverse {
		pairs[symbol] = pair
	}

	return pairs
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

func invertDec(d sdk.Dec) sdk.Dec {
	if d.IsZero() || d.IsNil() {
		return sdk.ZeroDec()
	}
	return sdk.NewDec(1).Quo(d)
}
