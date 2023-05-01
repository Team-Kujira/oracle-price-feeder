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

	provider.priceScales = map[string]float64{}
	provider.valueScales = map[string]float64{}

	content, err := provider.httpGet("/public/products")
	if err != nil {
		return nil, err
	}

	var info PhemexProductsResponse
	err = json.Unmarshal(content, &info)
	if err != nil {
		return nil, err
	}

	baseCurrencies := map[string]bool{}
	for _, pair := range pairs {
		baseCurrencies[pair.Base] = true
	}

	for _, currency := range info.Data.Currencies {
		_, ok := baseCurrencies[currency.Denom]
		if !ok {
			continue
		}
		provider.valueScales[currency.Denom] = float64(currency.ValueScale)
	}

	for _, product := range info.Data.Products {
		symbol := product.Base + product.Quote
		_, ok := provider.pairs[symbol]
		if !ok {
			continue
		}
		provider.priceScales[symbol] = float64(product.PriceScale)
	}

	// rate limit 100req/min ~1.66req/s
	interval := time.Duration(len(pairs)*1700+2000) * time.Millisecond

	go startPolling(provider, interval, logger)
	return provider, nil
}

func (p *PhemexProvider) Poll() error {
	for _, pair := range p.pairs {
		go func(p *PhemexProvider, pair types.CurrencyPair) {
			symbol := pair.String()

			content, err := p.httpGet("/md/spot/ticker/24hr?symbol=s" + symbol)
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

			valueScale, ok := p.valueScales[pair.Base]
			if !ok {
				p.logger.Error().
					Str("denom", pair.Base).
					Msg("no value scale")
				return
			}

			price := float64(ticker.Result.Price) / math.Pow(10, valueScale)
			volume := float64(ticker.Result.Volume) / math.Pow(10, priceScale)

			p.mtx.Lock()
			defer p.mtx.Unlock()

			p.tickers[pair.String()] = types.TickerPrice{
				Price:  floatToDec(price),
				Volume: floatToDec(volume),
				Time:   time.UnixMicro(int64(ticker.Result.Time)),
			}

		}(p, pair)
	}

	p.logger.Debug().Msg("updated tickers")
	return nil
}
