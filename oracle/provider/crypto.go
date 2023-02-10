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
		Rest:         "https://api.crypto.com",
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
	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *CryptoProvider) Poll() error {
	symbols := make(map[string]string, len(p.pairs))
	for _, pair := range p.pairs {
		symbols[pair.Join("_")] = pair.String()
	}

	url := p.endpoints.Rest + "/v2/public/get-ticker"

	content, err := p.makeHttpRequest(url)
	if err != nil {
		return err
	}

	var tickers CryptoTickersResponse
	err = json.Unmarshal(content, &tickers)
	if err != nil {
		return err
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()
	for _, ticker := range tickers.Result.Data {
		symbol, ok := symbols[ticker.Symbol]
		if !ok {
			continue
		}

		p.tickers[symbol] = types.TickerPrice{
			Price:  strToDec(ticker.Price),
			Volume: strToDec(ticker.Volume),
			Time:   time.UnixMilli(ticker.Time),
		}
	}
	p.logger.Debug().Msg("updated tickers")
	return nil
}
