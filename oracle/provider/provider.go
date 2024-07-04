package provider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"price-feeder/oracle/provider/volume"
	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/rs/zerolog"
	"golang.org/x/crypto/sha3"
)

const (
	defaultTimeout       = 10 * time.Second
	staleTickersCutoff   = 1 * time.Minute
	providerCandlePeriod = 10 * time.Minute

	ProviderAstroportInjective Name = "astroport_injective"
	ProviderAstroportNeutron   Name = "astroport_neutron"
	ProviderAstroportTerra2    Name = "astroport_terra2"
	ProviderBinance            Name = "binance"
	ProviderBinanceUS          Name = "binanceus"
	ProviderBingx              Name = "bingx"
	ProviderBitfinex           Name = "bitfinex"
	ProviderBitforex           Name = "bitforex"
	ProviderBitget             Name = "bitget"
	ProviderBitmart            Name = "bitmart"
	ProviderBitstamp           Name = "bitstamp"
	ProviderBkex               Name = "bkex"
	ProviderBybit              Name = "bybit"
	ProviderCamelotV2          Name = "camelotv2"
	ProviderCamelotV3          Name = "camelotv3"
	ProviderCoinbase           Name = "coinbase"
	ProviderCoinex             Name = "coinex"
	ProviderCrypto             Name = "crypto"
	ProviderCurve              Name = "curve"
	ProviderDexter             Name = "dexter"
	ProviderFin                Name = "fin"
	ProviderFinV2              Name = "finv2"
	ProviderGate               Name = "gate"
	ProviderHelix              Name = "helix"
	ProviderHitBtc             Name = "hitbtc"
	ProviderHuobi              Name = "huobi"
	ProviderIdxOsmosis         Name = "idxosmosis"
	ProviderKraken             Name = "kraken"
	ProviderKucoin             Name = "kucoin"
	ProviderLbank              Name = "lbank"
	ProviderMaya               Name = "maya"
	ProviderMexc               Name = "mexc"
	ProviderMock               Name = "mock"
	ProviderOkx                Name = "okx"
	ProviderOsmosis            Name = "osmosis"
	ProviderOsmosisV2          Name = "osmosisv2"
	ProviderPancakeV3Bsc       Name = "pancakev3_bsc"
	ProviderPhemex             Name = "phemex"
	ProviderPionex             Name = "pionex"
	ProviderPoloniex           Name = "poloniex"
	ProviderPyth               Name = "pyth"
	ProviderShade              Name = "shade"
	ProviderStride             Name = "stride"
	ProviderUniswapV3          Name = "uniswapv3"
	ProviderUnstake            Name = "unstake"
	ProviderVelodromeV2        Name = "velodromev2"
	ProviderWhitewhaleCmdx     Name = "whitewhale_cmdx"
	ProviderWhitewhaleHuahua   Name = "whitewhale_huahua"
	ProviderWhitewhaleInj      Name = "whitewhale_inj"
	ProviderWhitewhaleJuno     Name = "whitewhale_juno"
	ProviderWhitewhaleLuna     Name = "whitewhale_luna"
	ProviderWhitewhaleLunc     Name = "whitewhale_lunc"
	ProviderWhitewhaleSei      Name = "whitewhale_sei"
	ProviderWhitewhaleWhale    Name = "whitewhale_whale"
	ProviderXt                 Name = "xt"
	ProviderZero               Name = "zero"
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
		name      string
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
		db        *sql.DB
		volumes   volume.VolumeHandler
		height    uint64
		chain     string
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
		VolumeBlocks      int
		VolumePause       int
		Decimals          map[string]int
		Periods           map[string]int
	}

	EvmLog struct {
		Address string   `json:"address"`
		Topics  []string `json:"topics"`
		Data    string   `json:"data"`
		Number  string   `json:"blockNumber"`
		Height  uint64
	}

	EvmBlock struct {
		Timestamp string `json:"timestamp"`
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

	// remove trailing slashes
	for i, url := range p.endpoints.Urls {
		p.endpoints.Urls[i] = strings.TrimRight(url, "/")
	}

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

	// set contract<>symbol mapping

	p.contracts = endpoints.ContractAddresses

	for symbol, contract := range endpoints.ContractAddresses {
		p.contracts[contract] = symbol
	}

	p.height = 0

	if p.db == nil {
		return
	}

	// set up volume handler

	symbols := []string{}
	for _, pair := range pairs {
		skip := false
		for _, symbol := range []string{pair.Base, pair.Quote} {
			_, found := endpoints.Decimals[symbol]
			if !found {
				skip = true
				logger.Debug().
					Str("symbol", symbol).
					Msg("unknown decimal")
			}
		}
		if skip {
			continue
		}

		symbols = append(symbols, pair.Base+pair.Quote)
		symbols = append(symbols, pair.Quote+pair.Base)
	}

	var period int64 = 86400
	name := endpoints.Name.String()

	volumes, err := volume.NewVolumeHandler(logger, p.db, name, symbols, period)
	if err != nil {
		panic(err)
	}

	p.volumes = volumes
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

func (p *provider) compactJsonString(message string) (string, error) {
	buffer := new(bytes.Buffer)
	err := json.Compact(buffer, []byte(message))
	if err != nil {
		p.logger.Err(err).Msg("")
		return "", err
	}

	return buffer.String(), nil
}

func (p *provider) getCosmosTx(hash string) (types.CosmosTx, error) {
	type (
		Attribute struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}

		Event struct {
			Type       string      `json:"type"`
			Attributes []Attribute `json:"attributes"`
		}

		TxResponse struct {
			Code   int64   `json:"code"`
			Height string  `json:"height"`
			Time   string  `json:"timestamp"`
			Events []Event `json:"events"`
		}

		Response struct {
			TxResponse TxResponse `json:"tx_response"`
		}
	)

	var tx types.CosmosTx

	path := fmt.Sprintf("/cosmos/tx/v1beta1/txs/%s", hash)

	content, err := p.httpGet(path)
	if err != nil {
		return tx, err
	}

	var response Response

	err = json.Unmarshal(content, &response)
	if err != nil {
		return tx, err
	}

	timestamp, err := time.Parse(time.RFC3339, response.TxResponse.Time)
	if err != nil {
		return tx, err
	}

	height, err := strconv.ParseUint(response.TxResponse.Height, 10, 64)
	if err != nil {
		return tx, err
	}

	events := make([]types.CosmosTxEvent, len(response.TxResponse.Events))
	for i, event := range response.TxResponse.Events {
		events[i] = types.CosmosTxEvent{
			Type:       event.Type,
			Attributes: map[string]string{},
		}

		for _, attribute := range event.Attributes {
			events[i].Attributes[attribute.Key] = attribute.Value
		}
	}

	tx = types.CosmosTx{
		Code:   response.TxResponse.Code,
		Time:   timestamp,
		Height: height,
		Events: events,
		Hash:   hash,
	}

	return tx, nil
}

func (p *provider) getCosmosHeight() (uint64, error) {
	path := "/cosmos/base/tendermint/v1beta1/blocks/latest"
	content, err := p.httpGet(path)
	if err != nil {
		return 0, err
	}

	var response types.CosmosBlockResponse

	err = json.Unmarshal(content, &response)
	if err != nil {
		return 0, err
	}

	height, err := strconv.ParseUint(response.Block.Header.Height, 10, 64)
	if err != nil {
		return 0, err
	}

	return height, nil
}

func (p *provider) getCosmosTxs(
	height uint64,
	msgTypes []string,
) ([]types.CosmosTx, time.Time, error) {
	var timestamp time.Time
	if height == 0 {
		return nil, timestamp, fmt.Errorf("height is 0")
	}

	var response types.CosmosBlockResponse

	path := fmt.Sprintf("/cosmos/tx/v1beta1/txs/block/%d", height)
	content, err := p.httpGet(path)
	if err != nil {
		return nil, timestamp, err
	}

	err = json.Unmarshal(content, &response)
	if err != nil {
		return nil, timestamp, err
	}

	timestamp, err = time.Parse(time.RFC3339, response.Block.Header.Time)
	if err != nil {
		return nil, timestamp, err
	}

	hashes := []string{}
	filter := map[string]struct{}{}
	for _, msgType := range msgTypes {
		filter[msgType] = struct{}{}
	}

	for i, tx := range response.Txs {
		for _, message := range tx.Body.Messages {
			_, found := filter[message.Type]
			if !found {
				continue
			}

			data, err := base64.StdEncoding.DecodeString(
				response.Block.Data.Txs[i],
			)
			if err != nil {
				return nil, timestamp, err
			}

			hash := sha256.New()
			hash.Write(data)
			hashes = append(hashes, fmt.Sprintf("%X", hash.Sum(nil)))
		}
	}

	txs := []types.CosmosTx{}
	for _, hash := range hashes {
		tx, err := p.getCosmosTx(hash)
		if err != nil {
			return nil, timestamp, err
		}
		txs = append(txs, tx)
	}

	return txs, timestamp, nil
}

func (p *provider) wasmRawQuery(contract, message string) ([]byte, error) {
	bz := append([]byte{0}, []byte(message)...)
	query := base64.StdEncoding.EncodeToString(bz)

	path := fmt.Sprintf(
		"/cosmwasm/wasm/v1/contract/%s/raw/%s",
		contract, query,
	)

	content, err := p.httpGet(path)
	if err != nil {
		p.logger.Err(err).Msg("")
		return nil, err
	}

	var response struct {
		Data string `json:"data"`
	}

	err = json.Unmarshal(content, &response)
	if err != nil {
		return nil, err
	}

	data, err := base64.StdEncoding.DecodeString(response.Data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (p *provider) wasmSmartQuery(contract, message string) ([]byte, error) {
	message, err := p.compactJsonString(message)
	if err != nil {
		return nil, err
	}

	query := base64.StdEncoding.EncodeToString([]byte(message))

	path := fmt.Sprintf(
		"/cosmwasm/wasm/v1/contract/%s/smart/%s",
		contract, query,
	)

	content, err := p.httpGet(path)
	if err != nil {
		p.logger.Err(err).Msg("")
		return nil, err
	}

	return content, nil
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
		index := 0
		urls := []string{}

		for i, url := range p.endpoints.Urls {
			if p.httpBase == url {
				index = i
				break
			}
		}

		urls = append(urls, p.endpoints.Urls[index+1:]...)
		urls = append(urls, p.endpoints.Urls[:index]...)

		for _, endpoint := range urls {
			p.logger.Warn().
				Str("endpoint", endpoint).
				Msg("trying alternate http endpoints")

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
			Str("body", string(body)).
			Str("url", url).
			Str("method", method).
			Msg("http request returned invalid status")
		if res.StatusCode == 429 || res.StatusCode == 418 {
			p.logger.Warn().
				Str("url", url).
				Str("retry_after", res.Header.Get("Retry-After")).
				Msg("http ratelimited")
		}
		return nil, fmt.Errorf("http request returned invalid status")
	}
	content, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if len(content) == 0 {
		return nil, fmt.Errorf("empty response")
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
	case ProviderBingx:
		defaults = bingxDefaultEndpoints
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
	case ProviderCoinex:
		defaults = coinexDefaultEndpoints
	case ProviderCrypto:
		defaults = cryptoDefaultEndpoints
	case ProviderCurve:
		defaults = curveDefaultEndpoints
	case ProviderDexter:
		defaults = dexterDefaultEndpoints
	case ProviderFin:
		defaults = finDefaultEndpoints
	case ProviderFinV2:
		defaults = finV2DefaultEndpoints
	case ProviderGate:
		defaults = gateDefaultEndpoints
	case ProviderHelix:
		defaults = helixDefaultEndpoints
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
	case ProviderMaya:
		defaults = mayaDefaultEndpoints
	case ProviderMock:
		defaults = mockDefaultEndpoints
	case ProviderOkx:
		defaults = okxDefaultEndpoints
	case ProviderOsmosis:
		defaults = osmosisDefaultEndpoints
	case ProviderOsmosisV2:
		defaults = osmosisv2DefaultEndpoints
	case ProviderPancakeV3Bsc:
		defaults = PancakeV3BscDefaultEndpoints
	case ProviderPhemex:
		defaults = phemexDefaultEndpoints
	case ProviderPionex:
		defaults = pionexDefaultEndpoints
	case ProviderPoloniex:
		defaults = poloniexDefaultEndpoints
	case ProviderPyth:
		defaults = pythDefaultEndpoints
	case ProviderShade:
		defaults = shadeDefaultEndpoints
	case ProviderUniswapV3:
		defaults = uniswapv3DefaultEndpoints
	case ProviderUnstake:
		defaults = unstakeDefaultEndpoints
	case ProviderVelodromeV2:
		defaults = velodromev2DefaultEndpoints
	case ProviderWhitewhaleCmdx:
		defaults = whitewhaleCmdxDefaultEndpoints
	case ProviderWhitewhaleHuahua:
		defaults = whitewhaleHuahuaDefaultEndpoints
	case ProviderWhitewhaleInj:
		defaults = whitewhaleInjDefaultEndpoints
	case ProviderWhitewhaleJuno:
		defaults = whitewhaleJunoDefaultEndpoints
	case ProviderWhitewhaleLunc:
		defaults = whitewhaleLuncDefaultEndpoints
	case ProviderWhitewhaleLuna:
		defaults = whitewhaleLunaDefaultEndpoints
	case ProviderWhitewhaleSei:
		defaults = whitewhaleSeiDefaultEndpoints
	case ProviderWhitewhaleWhale:
		defaults = whitewhaleWhaleDefaultEndpoints
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

	if e.VolumeBlocks == 0 {
		e.VolumeBlocks = defaults.VolumeBlocks
	}

	if e.VolumePause <= 0 {
		e.VolumePause = defaults.VolumePause
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

func (p *provider) setTickerPrice(
	symbol string,
	price sdk.Dec,
	volume sdk.Dec,
	timestamp time.Time,
) {
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
	for symbol := range p.endpoints.ContractAddresses {
		symbols[symbol] = struct{}{}
	}
	return symbols, nil
}

func (p *provider) getPair(symbol string) (types.CurrencyPair, bool) {
	pair, found := p.pairs[symbol]
	if found {
		return pair, true
	}

	pair, found = p.inverse[symbol]
	if found {
		return pair.Swap(), true
	}

	p.logger.Debug().
		Str("symbol", symbol).
		Msg("pair not found")

	return pair, false
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

// EVM specific

func (p *provider) doEthCall(address, data string) (string, error) {
	type Body struct {
		Jsonrpc string        `json:"jsonrpc"`
		Method  string        `json:"method"`
		Params  []interface{} `json:"params"`
		Id      int64         `json:"id"`
	}

	type Transaction struct {
		To   string `json:"to"`
		Data string `json:"data"`
	}

	type Response struct {
		Result string `json:"result"` // Encoded data ex.: 0x0000000000000...
	}

	data = "0x" + strings.ReplaceAll(data, "0x", "")

	body := Body{
		Jsonrpc: "2.0",
		Method:  "eth_call",
		Params: []interface{}{
			Transaction{
				To:   address,
				Data: data,
			},
			"latest",
		},
		Id: 1,
	}

	bz, err := json.Marshal(body)
	if err != nil {
		p.logger.Err(err).Msg("")
		return "", err
	}

	content, err := p.httpPost("", bz)
	if err != nil {
		p.logger.Err(err).Msg("")
		return "", err
	}

	var response Response
	err = json.Unmarshal(content, &response)
	if err != nil {
		p.logger.Err(err).Msg("")
		return "", err
	}

	return response.Result, nil
}

func (p *provider) getEthDecimals(contract string) (uint64, error) {
	p.logger.Info().Str("contract", contract).Msg("get decimals")

	hash, err := keccak256("decimals()")
	if err != nil {
		return 0, err
	}

	data := fmt.Sprintf("%0.8s%064d", hash, 0)

	result, err := p.doEthCall(contract, data)
	if err != nil {
		return 0, err
	}

	types := []string{"uint8"}

	decoded, err := decodeEthData(result, types)
	if err != nil {
		return 0, err
	}

	decimals, err := strconv.ParseUint(fmt.Sprintf("%v", decoded[0]), 10, 8)
	if err != nil {
		return 0, err
	}

	return decimals, nil
}

func (p *provider) evmRpcQuery(method, params string) (json.RawMessage, error) {
	p.logger.Info().Str("method", method).Msg("query evm rpc")

	query := []byte(fmt.Sprintf(
		`{"jsonrpc":"2.0","method":"%s","params":[%s],"id":1}`,
		method, string(params),
	))

	p.logger.Debug().Msg(string(query))

	TelemetryEvmMethod(p.chain, p.name, method)

	content, err := p.httpPost("", query)
	if err != nil {
		return nil, p.error(err)
	}

	var response struct {
		Result json.RawMessage `json:"result"`
	}

	err = json.Unmarshal(content, &response)
	if err != nil {
		return nil, p.error(err)
	}

	if len(response.Result) == 0 {
		p.logger.Error().
			Str("content", string(content)).
			Msg("empty return data")
	}

	return response.Result, nil
}

func (p *provider) getEvmHeight() (uint64, error) {
	p.logger.Info().Msg("get height")

	result, err := p.evmRpcQuery("eth_blockNumber", "")
	if err != nil {
		return 0, err
	}

	var height string
	err = json.Unmarshal(result, &height)
	if err != nil {
		return 0, err
	}
	// strip "0x" from height string
	return strconv.ParseUint(height[2:], 16, 64)
}

func (p *provider) evmGetLogs(
	from, to uint64,
	addresses, topics []string,
) ([]EvmLog, error) {
	// Note: getLogs returns values between and includin from- and end block
	// -> from=1, to=3 returns [1, 2, 3]

	type Params struct {
		FromBlock string   `json:"fromBlock"`
		ToBlock   string   `json:"toBlock"`
		Address   []string `json:"address"`
		Topics    []string `json:"topics"`
	}

	params := Params{
		FromBlock: fmt.Sprintf("0x%x", from),
		ToBlock:   fmt.Sprintf("0x%x", to),
		Address:   addresses,
		Topics:    topics,
	}

	if to == 0 {
		params.ToBlock = "latest"
	}

	bz, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}

	response, err := p.evmRpcQuery("eth_getLogs", string(bz))
	if err != nil {
		return nil, err
	}

	var logs []EvmLog
	err = json.Unmarshal(response, &logs)
	if err != nil {
		p.logger.Err(err).Msg("failed to unmarshal evm log")
		return nil, err
	}

	for i, log := range logs {
		height, err := hexToUint64(log.Number)
		if err != nil {
			return nil, err
		}
		logs[i].Height = height
	}

	return logs, nil
}

func (p *provider) evmGetBlockByNumber(height uint64) (EvmBlock, error) {
	params := fmt.Sprintf(`"0x%x",false`, height)
	output, err := p.evmRpcQuery("eth_getBlockByNumber", params)
	if err != nil {
		return EvmBlock{}, err
	}

	var block EvmBlock

	err = json.Unmarshal(output, &block)
	if err != nil {
		return EvmBlock{}, err
	}

	return block, nil
}

func (p *provider) evmCall(
	address, method string,
	args []string,
) (json.RawMessage, error) {
	p.logger.Info().Str("method", method).Msg("evmCall")

	hash, err := keccak256(method)
	if err != nil {
		return nil, p.error(err)
	}

	data := "0x" + hash[:8]

	for _, arg := range args {
		data += arg
	}

	if len(args) == 0 {
		// append 64 zeros
		data = fmt.Sprintf("%s%064d", data, 0)
	}

	params := fmt.Sprintf(`{"to":"%s","data":"%s"},"latest"`, address, data)

	return p.evmRpcQuery("eth_call", params)
}

func (p *provider) error(err error) error {
	p.logger.Err(err).Msg("")
	return err
}

func (p *provider) errorf(message string) error {
	err := fmt.Errorf(message)
	return p.error(err)
}

func hexToUint64(s string) (uint64, error) {
	return strconv.ParseUint(strings.Replace(s, "0x", "", -1), 16, 64)
}

func decodeSqrtPrice(sqrt string) (sdk.Dec, error) {
	dec := strToDec(sqrt)
	if dec.IsZero() {
		return sdk.Dec{}, fmt.Errorf("sqrt is 0")
	}
	return dec.Power(2).Quo(uintToDec(2).Power(192)), nil
}

func decodeEthData(data string, types []string) ([]interface{}, error) {
	type AbiOutput struct {
		Name         string `json:"name"`
		Type         string `json:"type"`
		InternalType string `json:"internalType"`
	}

	type AbiDefinition struct {
		Name    string      `json:"name"`
		Type    string      `json:"type"`
		Outputs []AbiOutput `json:"outputs"`
	}

	outputs := []AbiOutput{}
	for _, t := range types {
		outputs = append(outputs, AbiOutput{
			Name:         "",
			Type:         t,
			InternalType: t,
		})
	}

	definition, err := json.Marshal([]AbiDefinition{{
		Name:    "fn",
		Type:    "function",
		Outputs: outputs,
	}})
	if err != nil {
		return nil, err
	}

	abi, err := abi.JSON(strings.NewReader(string(definition)))
	if err != nil {
		return nil, err
	}

	data = strings.TrimPrefix(data, "0x")

	decoded, err := hex.DecodeString(data)
	if err != nil {
		return nil, err
	}

	return abi.Unpack("fn", decoded)
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
	if len(str) == 0 {
		return sdk.Dec{}
	}

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

func int64ToDec(i int64) sdk.Dec {
	return strToDec(strconv.FormatInt(i, 10))
}

func uintToDec(u uint64) sdk.Dec {
	return strToDec(strconv.FormatUint(u, 10))
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

func computeDecimalsFactor(base, quote int64) (sdk.Dec, error) {
	delta := base - quote
	factor := uintToDec(1)
	if delta == 0 {
		return factor, nil
	} else if delta < 0 {
		factor = factor.Quo(uintToDec(10).Power(uint64(delta * -1)))
	} else {
		factor = uintToDec(10).Power(uint64(delta))
	}

	return factor, nil
}

func keccak256(s string) (string, error) {
	hash := sha3.NewLegacyKeccak256()
	_, err := hash.Write([]byte(s))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func parseDenom(s string) (sdk.Dec, string, error) {
	re := regexp.MustCompile(`^([0-9]+)(.*)$`)

	matches := re.FindAllStringSubmatch(s, -1)[0]
	if len(matches) != 3 {
		return sdk.Dec{}, "", fmt.Errorf("failed parsing denom")
	}

	amount := strToDec(matches[1])

	return amount, matches[2], nil
}

func (b *EvmBlock) GetTime() (time.Time, error) {
	seconds, err := strconv.ParseInt(
		strings.Replace(b.Timestamp, "0x", "", -1), 16, 64,
	)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(seconds, 0), nil
}
