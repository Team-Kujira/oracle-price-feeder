package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

var (
	_                       Provider = (*UnstakeProvider)(nil)
	unstakeDefaultEndpoints          = Endpoint{
		Name:         ProviderUnstake,
		Urls:         []string{},
		PollInterval: 4 * time.Second,
	}
)

type (
	// Unstake defines an oracle provider that queries Unstake.fi contracts
	UnstakeProvider struct {
		provider
		contracts map[string]string
		periods   map[string]sdk.Dec
		decimals  map[string]uint64
	}
)

func NewUnstakeProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*UnstakeProvider, error) {
	provider := &UnstakeProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	provider.contracts = provider.endpoints.ContractAddresses

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, nil)

	provider.periods = map[string]sdk.Dec{}
	for symbol, period := range endpoints.Periods {
		provider.periods[symbol] = uintToDec(uint64(period))
	}

	provider.decimals = map[string]uint64{}
	for symbol, decimals := range endpoints.Decimals {
		provider.decimals[symbol] = uint64(decimals)
	}

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *UnstakeProvider) Poll() error {
	timestamp := time.Now()

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for symbol, pair := range p.getAllPairs() {

		contract, err := p.getContractAddress(pair)
		if err != nil {
			p.logger.Warn().
				Str("symbol", symbol).
				Msg("no contract address found")
			continue
		}

		ask, bid, err := p.getRates(contract)
		if err != nil {
			return err
		}

		price := bid.Add(ask).QuoInt64(2)

		unbonding, err := p.getUnbonding(contract, ask)
		if err != nil {
			return err
		}

		decimals, found := p.decimals[pair.Quote]
		if !found {
			err = fmt.Errorf("decimals not found")
			p.logger.Err(err).Msg("")
			return err
		}

		period, found := p.periods[pair.String()]
		if !found {
			err = fmt.Errorf("no unbonding period found")
			p.logger.Err(err).Msg("")
			return err
		}

		p.setTickerPrice(
			symbol,
			price,
			unbonding.Quo(period).Quo(uintToDec(10).Power(decimals)),
			timestamp,
		)
	}

	return nil
}

func (p *UnstakeProvider) GetAvailablePairs() (map[string]struct{}, error) {
	return p.getAvailablePairsFromContracts()
}

func (p *UnstakeProvider) getUnbonding(
	contract string,
	rate sdk.Dec,
) (sdk.Dec, error) {
	var unbonding sdk.Dec

	var statusResponse struct {
		Data struct {
			TotalBase  string `json:"total_base"`
			TotalQuote string `json:"total_quote"`
		} `json:"data"`
	}

	content, err := p.wasmSmartQuery(contract, `{"status":{}}`)
	if err != nil {
		return unbonding, err
	}

	err = json.Unmarshal(content, &statusResponse)
	if err != nil {
		return unbonding, err
	}

	totalBase := strToDec(statusResponse.Data.TotalBase)
	totalQuote := strToDec(statusResponse.Data.TotalQuote)

	unbonding = totalBase.Mul(rate).Sub(totalQuote)

	return unbonding, nil
}

func (p *UnstakeProvider) getRates(contract string) (sdk.Dec, sdk.Dec, error) {
	var ask, bid sdk.Dec

	var ratesResponse struct {
		Data struct {
			ProviderRedemption string `json:"provider_redemption"`
		} `json:"data"`
	}

	var offerResponse struct {
		Data struct {
			Amount string `json:"amount"`
		} `json:"data"`
	}

	content, err := p.wasmSmartQuery(contract, `{"rates":{}}`)
	if err != nil {
		return ask, bid, err
	}

	err = json.Unmarshal(content, &ratesResponse)
	if err != nil {
		return ask, bid, err
	}

	amount := uint64(1_000_000_000)
	query := fmt.Sprintf(`{"offer":{"amount": "%d"}}`, amount)
	content, err = p.wasmSmartQuery(contract, query)
	if err != nil {
		return ask, bid, err
	}

	err = json.Unmarshal(content, &offerResponse)
	if err != nil {
		return ask, bid, err
	}

	ask = strToDec(ratesResponse.Data.ProviderRedemption)
	bid = strToDec(offerResponse.Data.Amount).Quo(uintToDec(amount))

	return ask, bid, nil
}
