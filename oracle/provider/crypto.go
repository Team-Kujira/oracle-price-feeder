package provider

import (
	"context"
	"encoding/json"
	"time"

	"price-feeder/oracle/types"

	"github.com/rs/zerolog"
)

var (
	_                      Provider = (*CryptoProvider)(nil)
	cryptoDefaultEndpoints          = Endpoint{
		Name:         ProviderCrypto,
		Urls:         []string{"https://api.crypto.com"},
		PollInterval: 2 * time.Second,
	}
)

type (
	// CryptoProvider defines an oracle provider implemented by the crypto.com
	// public API.
	//
	// REF: https://exchange-docs.crypto.com/spot/index.html
	CryptoProvider struct {
		provider
	}

	CryptoTickersResponse struct {
		Code   int64                     `json:"code"`
		Result CryptoTickersResponseData `json:"result"`
	}

	CryptoTickersResponseData struct {
		Data []CryptoTicker `json:"data"`
	}

	CryptoTicker struct {
		Symbol string `json:"i"` // Symbol ex.: BTC_USDT
		Price  string `json:"a"` // Last price ex.: 0.0025
		Volume string `json:"v"` // Total traded base asset volume ex.: 1000
		Time   int64  `json:"t"` // Timestamp ex.: 1675246930699
	}
)

func NewCryptoProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*CryptoProvider, error) {
	provider := &CryptoProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, currencyPairToCryptoSymbol)

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *CryptoProvider) getTickers() (CryptoTickersResponse, error) {
	content, err := p.httpGet("/v2/public/get-ticker")
	if err != nil {
		return CryptoTickersResponse{}, err
	}

	var tickers CryptoTickersResponse
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return CryptoTickersResponse{}, err
	}

	return tickers, nil
}

func (p *CryptoProvider) Poll() error {
	tickers, err := p.getTickers()
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()
	for _, ticker := range tickers.Result.Data {
		if !p.isPair(ticker.Symbol) {
			continue
		}

		p.setTickerPrice(
			ticker.Symbol,
			strToDec(ticker.Price),
			strToDec(ticker.Volume),
			time.UnixMilli(ticker.Time),
		)
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}

func (p *CryptoProvider) GetAvailablePairs() (map[string]struct{}, error) {
	tickers, err := p.getTickers()
	if err != nil {
		return nil, err
	}

	symbols := map[string]struct{}{}
	for _, ticker := range tickers.Result.Data {
		symbols[ticker.Symbol] = struct{}{}
	}

	return symbols, nil
}

func currencyPairToCryptoSymbol(pair types.CurrencyPair) string {
	return pair.Join("_")
}
