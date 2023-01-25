package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"

	"price-feeder/oracle/types"
)

const (
	coinbaseRestHost   = "https://api.exchange.coinbase.com"
	coinbaseTimeFormat = "2006-01-02T15:04:05.000000Z"
)

var (
	_ Provider = (*CoinbaseProvider)(nil)
)

type (
	// CoinbaseProvider defines an Oracle provider implemented by the Coinbase public
	// API.
	//
	// REF: https://www.coinbase.io/docs/websocket/index.html
	CoinbaseProvider struct {
		rac             *GenericRequestController
		logger          zerolog.Logger
		mtx             sync.RWMutex
		endpoints       Endpoint
		client          *http.Client
		tickers         map[string]types.TickerPrice  // Symbol => CoinbaseWsTickerMsg
		subscribedPairs map[string]types.CurrencyPair // Symbol => types.CurrencyPair
	}

	CoinbaseRestTickerResponse struct {
		Price  string `json:"price"`
		Volume string `json:"volume"`
		Time   string `json:"time"`
	}
)

// NewCoinbaseProvider creates a new CoinbaseProvider.
func NewCoinbaseProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*CoinbaseProvider, error) {
	if endpoints.Name != ProviderCoinbase {
		endpoints = Endpoint{
			Name: ProviderCoinbase,
			Rest: coinbaseRestHost,
		}
	}

	coinbaseLogger := logger.With().Str("provider", string(ProviderCoinbase)).Logger()

	provider := &CoinbaseProvider{
		logger:          coinbaseLogger,
		endpoints:       endpoints,
		client:          newDefaultHTTPClient(),
		tickers:         map[string]types.TickerPrice{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	provider.SubscribeCurrencyPairs(pairs...)

	provider.rac = NewGenericRequestController(
		ctx,
		time.Second*5,
		provider.requestPrices,
		coinbaseLogger,
	)

	go provider.rac.Start()

	return provider, nil
}

func (p *CoinbaseProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	return nil
}

func (p *CoinbaseProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	cp, ok := p.subscribedPairs[s]
	return cp, ok
}

func (p *CoinbaseProvider) SetSubscribedPair(cp types.CurrencyPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.subscribedPairs[cp.String()] = cp
}

func (p *CoinbaseProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	// adjust request interval
	interval := math.Ceil(float64(len(cps)) / 10 * 3)
	p.logger.Debug().
		Float64("interval", interval).
		Msg("set request interval")

	return subscribeCurrencyPairs(p, cps)
}

func (p *CoinbaseProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	return nil
}

func (p *CoinbaseProvider) requestPrices() error {
	i := 0
	for _, cp := range p.subscribedPairs {
		go func(p *CoinbaseProvider, cp types.CurrencyPair) {
			url := p.endpoints.Rest + "/products/" + cp.Join("-") + "/ticker"

			p.logger.Debug().Msg("get " + url)

			resp, err := p.client.Get(url)
			if err != nil {
				p.logger.Error().Msg("failed to get tickers")
				return
			}
			defer resp.Body.Close()

			var tickerResp CoinbaseRestTickerResponse
			err = json.NewDecoder(resp.Body).Decode(&tickerResp)
			if err != nil {
				p.logger.Error().Msg("failed to parse rest response")
				return
			}

			timestamp, err := time.Parse(coinbaseTimeFormat, tickerResp.Time)
			if err != nil {
				p.logger.Error().Msg("failed to parse timestamp")
				return
			}

			ticker := types.TickerPrice{
				Price:  sdk.MustNewDecFromStr(tickerResp.Price),
				Volume: sdk.MustNewDecFromStr(tickerResp.Volume),
				Time:   timestamp.UnixMilli(),
			}

			p.setTicker(cp.String(), ticker)
		}(p, cp)

		i = i + 1

		if i == 10 {
			p.logger.Debug().Msg("sleep for 1.2s")
			i = 0
			time.Sleep(time.Millisecond * 1200)
		}
	}

	return nil
}

func (p *CoinbaseProvider) setTicker(symbol string, ticker types.TickerPrice) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.tickers[symbol] = ticker
}

// GetTickerPrices returns the tickerPrices based on the saved map.
func (p *CoinbaseProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	return getTickerPrices(p, cps)
}

func (p *CoinbaseProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	symbol := cp.String()
	ticker, ok := p.tickers[symbol]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf("coinbase failed to get ticker price for %s", symbol)
	}

	return ticker, nil
}

// GetAvailablePairs returns all pairs to which the provider can subscribe.
func (p *CoinbaseProvider) GetAvailablePairs() (map[string]struct{}, error) {
	// not used yet, so skipping this unless needed
	return make(map[string]struct{}, 0), nil
}
