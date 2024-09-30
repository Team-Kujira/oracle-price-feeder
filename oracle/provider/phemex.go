package provider

import (
	"context"
	"encoding/json"
	"math"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                      Provider = (*PhemexProvider)(nil)
	phemexDefaultEndpoints          = Endpoint{
		Name:         ProviderPhemex,
		Urls:         []string{"https://api.phemex.com"},
		PollInterval: 3 * time.Second,
	}
)

type (
	// PhemexProvider defines an oracle provider implemented by the Phemex
	// public API.
	//
	// REF: https://phemex-docs.github.io
	PhemexProvider struct {
		provider
		priceScales map[string]float64
		valueScales map[string]float64
	}

	PhemexTickerResponse struct {
		Result PhemexTicker `json:"result"`
	}

	PhemexTicker struct {
		Symbol string `json:"symbol"`    // ex.: "sBTCUSDT"
		Price  uint64 `json:"lastEp"`    // ex.: 2323102000000
		Volume uint64 `json:"volumeEv"`  // ex.: 450522008300
		Time   int64  `json:"timestamp"` // ex.: 1675843104642440505
	}

	PhemexProductsResponse struct {
		Data PhemexProductsData `json:"data"`
	}

	PhemexProductsData struct {
		Currencies []PhemexCurrency `json:"currencies"`
		Products   []PhemexProduct  `json:"products"`
	}

	PhemexCurrency struct {
		Denom      string `json:"currency"`   // ex.: "BTC"
		ValueScale int64  `json:"valueScale"` // ex.: 8
	}

	PhemexProduct struct {
		Symbol     string `json:"symbol"`        // ex.: "sBTCUSDT"
		Base       string `json:"baseCurrency"`  // ex.: "BTC"
		Quote      string `json:"quoteCurrency"` // ex.: "USDT"
		PriceScale int64  `json:"priceScale"`    // ex.: 8
	}
)

func NewPhemexProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*PhemexProvider, error) {
	provider := &PhemexProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToPhemexSymbol)

	provider.priceScales = map[string]float64{}
	provider.valueScales = map[string]float64{}

	products, err := provider.getProducts()
	if err != nil {
		return nil, err
	}

	baseCurrencies := map[string]struct{}{}
	for _, pair := range provider.pairs {
		baseCurrencies[pair.Base] = struct{}{}
	}
	for _, pair := range provider.inverse {
		baseCurrencies[pair.Quote] = struct{}{}
	}

	for _, currency := range products.Data.Currencies {
		_, ok := baseCurrencies[currency.Denom]
		if !ok {
			continue
		}
		provider.valueScales[currency.Denom] = float64(currency.ValueScale)
	}

	for _, product := range products.Data.Products {
		if !provider.isPair(product.Symbol) {
			continue
		}
		provider.priceScales[product.Symbol] = float64(product.PriceScale)
	}

	// rate limit 100req/min ~1.66req/s
	interval := time.Duration(
		len(provider.getAllPairs())*1700+2000,
	) * time.Millisecond

	go startPolling(provider, interval, logger)
	return provider, nil
}

func (p *PhemexProvider) getProducts() (PhemexProductsResponse, error) {
	content, err := p.httpGet("/public/products")
	if err != nil {
		return PhemexProductsResponse{}, err
	}

	var products PhemexProductsResponse
	err = json.Unmarshal(content, &products)
	if err != nil {
		return PhemexProductsResponse{}, err
	}

	return products, nil
}

func (p *PhemexProvider) Poll() error {
	for symbol, pair := range p.getAllPairs() {
		go func(p *PhemexProvider, symbol string, pair types.CurrencyPair) {
			content, err := p.httpGet("/md/spot/ticker/24hr?symbol=" + symbol)
			if err != nil {
				p.logger.Error().
					Str("symbol", symbol).
					Err(err)
				return
			}

			var ticker PhemexTickerResponse
			err = json.Unmarshal(content, &ticker)
			if err != nil {
				p.logger.Error().
					Str("symbol", symbol).
					Err(err)
				return
			}

			priceScale, ok := p.priceScales[symbol]
			if !ok {
				p.logger.Error().
					Str("symbol", symbol).
					Msg("no price scale")
				return
			}

			if _, found := p.inverse[symbol]; found {
				pair = pair.Swap()
			}

			valueScale, ok := p.valueScales[pair.Base]
			if !ok {
				p.logger.Error().
					Str("denom", pair.Base).
					Msg("no value scale")
				return
			}

			price := float64(ticker.Result.Price) / math.Pow(10, priceScale)
			volume := float64(ticker.Result.Volume) / math.Pow(10, valueScale)

			p.mtx.Lock()
			defer p.mtx.Unlock()

			p.setTickerPrice(
				symbol,
				floatToDec(price),
				floatToDec(volume),
				time.UnixMicro(int64(ticker.Result.Time)),
			)
		}(p, symbol, pair)
	}

	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *PhemexProvider) GetAvailablePairs() (map[string]struct{}, error) {
	products, err := p.getProducts()
	if err != nil {
		return nil, err
	}

	symbols := map[string]struct{}{}
	for _, product := range products.Data.Products {
		if product.Symbol[0:1] == "s" {
			symbols[product.Symbol] = struct{}{}
		}
	}

	return symbols, nil
}

func currencyPairToPhemexSymbol(pair types.CurrencyPair) string {
	mapping := map[string]string{
		"MATIC": "POL",
	}

	base, found := mapping[pair.Base]
	if !found {
		base = pair.Base
	}

	quote, found := mapping[pair.Quote]
	if !found {
		quote = pair.Quote
	}

	return "s" + base + quote
}
