package provider

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
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

	ProviderAstroportTerra2    Name = "astroport_terra2"
	ProviderAstroportNeutron   Name = "astroport_neutron"
	ProviderAstroportInjective Name = "astroport_injective"
	ProviderFin                Name = "fin"
	ProviderFinV2              Name = "finv2"
	ProviderKraken             Name = "kraken"
	ProviderBinance            Name = "binance"
	ProviderBinanceUS          Name = "binanceus"
	ProviderCamelotV2          Name = "camelotv2"
	ProviderCamelotV3          Name = "camelotv3"
	ProviderOsmosis            Name = "osmosis"
	ProviderOsmosisV2          Name = "osmosisv2"
	ProviderHuobi              Name = "huobi"
	ProviderOkx                Name = "okx"
	ProviderGate               Name = "gate"
	ProviderCoinbase           Name = "coinbase"
	ProviderBitget             Name = "bitget"
	ProviderBitmart            Name = "bitmart"
	ProviderBkex               Name = "bkex"
	ProviderBitfinex           Name = "bitfinex"
	ProviderBitforex           Name = "bitforex"
	ProviderBitstamp           Name = "bitstamp"
	ProviderHitBtc             Name = "hitbtc"
	ProviderPoloniex           Name = "poloniex"
	ProviderPyth               Name = "pyth"
	ProviderPhemex             Name = "phemex"
	ProviderLbank              Name = "lbank"
	ProviderKucoin             Name = "kucoin"
	ProviderBybit              Name = "bybit"
	ProviderMexc               Name = "mexc"
	ProviderCrypto             Name = "crypto"
	ProviderCurve              Name = "curve"
	ProviderMock               Name = "mock"
	ProviderStride             Name = "stride"
	ProviderXt                 Name = "xt"
	ProviderIdxOsmosis         Name = "idxosmosis"
	ProviderZero               Name = "zero"
	ProviderUniswapV3          Name = "uniswapv3"
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
		contracts map[string]string
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
		Name              Name // ex. "binance"
		Urls              []string
		Websocket         string // ex. "stream.binance.com:9443"
		WebsocketPath     string
		PollInterval      time.Duration
		PingDuration      time.Duration
		PingType          uint
		PingMessage       string
		ContractAddresses map[string]string
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

	p.contracts = endpoints.ContractAddresses

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
				p.logger.Warn().
					Str("pair", symbol).
					Time("time", price.Time).
					Msg("tickers data is stale")
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
	return p.httpRequest(path, "GET", nil, nil)
}

func (p *provider) httpPost(path string, body []byte) ([]byte, error) {
	headers := map[string]string{
		"Content-Type": "application/json",
	}
	return p.httpRequest(path, "POST", body, headers)
}

func (p *provider) httpRequest(path string, method string, body []byte, headers map[string]string) ([]byte, error) {
	res, err := p.makeHttpRequest(p.httpBase+path, method, body, headers)
	if err != nil {
		p.logger.Warn().
			Str("endpoint", p.httpBase).
			Str("path", path).
			Msg("trying alternate http endpoints")
		for _, endpoint := range p.endpoints.Urls {
			if endpoint == p.httpBase {
				continue
			}
			res, err = p.makeHttpRequest(endpoint+path, method, body, headers)
			if err == nil {
				p.logger.Info().Str("endpoint", endpoint).Msg("selected alternate http endpoint")
				p.httpBase = endpoint
				break
			}
		}
	}
	return res, err
}

func (p *provider) makeHttpRequest(url string, method string, body []byte, headers map[string]string) ([]byte, error) {
	req, err := http.NewRequest(method, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	for key, value := range headers {
		req.Header.Set(key, value)
	}

	res, err := p.http.Do(req)
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
	case ProviderAstroportInjective:
		defaults = astroportInjectiveDefaultEndpoints
	case ProviderAstroportNeutron:
		defaults = astroportNeutronDefaultEndpoints
	case ProviderAstroportTerra2:
		defaults = astroportTerra2DefaultEndpoints
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
	case ProviderBitstamp:
		defaults = bitstampDefaultEndpoints
	case ProviderBkex:
		defaults = bkexDefaultEndpoints
	case ProviderBybit:
		defaults = bybitDefaultEndpoints
	case ProviderCamelotV2:
		defaults = camelotV2DefaultEndpoints
	case ProviderCamelotV3:
		defaults = camelotV3DefaultEndpoints
	case ProviderCoinbase:
		defaults = coinbaseDefaultEndpoints
	case ProviderCrypto:
		defaults = cryptoDefaultEndpoints
	case ProviderCurve:
		defaults = curveDefaultEndpoints
	case ProviderFin:
		defaults = finDefaultEndpoints
	case ProviderFinV2:
		defaults = finV2DefaultEndpoints
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
	case ProviderUniswapV3:
		defaults = uniswapv3DefaultEndpoints
	case ProviderXt:
		defaults = xtDefaultEndpoints
	case ProviderZero:
		defaults = zeroDefaultEndpoints
	default:
		return
	}
	if e.Urls == nil {
		urls := defaults.Urls
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(
			len(urls),
			func(i, j int) { urls[i], urls[j] = urls[j], urls[i] },
		)
		e.Urls = urls
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
	// add default contract addresses, if not already defined
	for symbol, address := range defaults.ContractAddresses {
		_, found := e.ContractAddresses[symbol]
		if found {
			continue
		}
		e.ContractAddresses[symbol] = address
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
	if price.IsNil() || price.LTE(sdk.ZeroDec()) {
		p.logger.Warn().
			Str("symbol", symbol).
			Msgf("price is %s", price)
		return
	}

	if volume.IsZero() {
		p.logger.Debug().
			Str("symbol", symbol).
			Msg("volume is zero")
	}

	// check if price needs to be inverted
	pair, inverse := p.inverse[symbol]
	if inverse {
		volume = volume.Mul(price)
		price = invertDec(price)

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

func (p *provider) getAvailablePairsFromContracts() (map[string]struct{}, error) {
	symbols := map[string]struct{}{}
	for symbol := range p.contracts {
		symbols[symbol] = struct{}{}
	}
	return symbols, nil
}

func (p *provider) getContractAddress(pair types.CurrencyPair) (string, error) {
	address, found := p.contracts[pair.String()]
	if found {
		return address, nil
	}

	address, found = p.contracts[pair.Quote+pair.Base]
	if found {
		return address, nil
	}

	err := fmt.Errorf("no contract address found")

	p.logger.Error().
		Str("pair", pair.String()).
		Err(err)

	return "", err
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
	dec, err := sdk.NewDecFromStr(str)
	if err != nil {
		dec = sdk.Dec{}
	}

	return dec
}

func floatToDec(f float64) sdk.Dec {
	return strToDec(strconv.FormatFloat(f, 'f', -1, 64))
}

func invertDec(d sdk.Dec) sdk.Dec {
	if d.IsZero() || d.IsNil() {
		return sdk.ZeroDec()
	}
	return sdk.NewDec(1).Quo(d)
}
