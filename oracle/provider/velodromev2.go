package provider

import (
	"context"
	"fmt"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

var (
	_                           Provider = (*VelodromeV2Provider)(nil)
	velodromev2DefaultEndpoints          = Endpoint{
		Name:         ProviderVelodromeV2,
		Urls:         []string{},
		PollInterval: 10 * time.Second,
	}
)

type (
	// VelodromeV2Provider defines an oracle provider calling velodrome pools
	// directly on optimism
	VelodromeV2Provider struct {
		provider
		decimals map[string]uint64
		symbols  map[string]string
	}

	VelodromeV2Response struct {
		Result string `json:"result"` // Encoded data ex.: 0x0000000000000...
	}
)

func NewVelodromeV2Provider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*VelodromeV2Provider, error) {
	provider := &VelodromeV2Provider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, nil)

	// get token decimals
	provider.setDecimals()

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *VelodromeV2Provider) Poll() error {
	types := []string{"uint256"}

	hash, err := keccak256("quote(address,uint256,uint256)")
	if err != nil {
		return err
	}

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

		data := fmt.Sprintf(
			// symbol has 0x prefix, dropping that
			"%s%064s%064d%064d", hash[:8], p.symbols[pair.Base][2:], 1, 1,
		)
		response, err := p.doEthCall(contract, data)
		if err != nil {
			p.logger.Err(err)
			continue
		}

		decoded, err := decodeEthData(response, types)
		if err != nil {
			p.logger.Err(err)
			continue
		}

		base := pair.Base
		quote := pair.Quote
		_, found := p.contracts[pair.String()]
		if !found {
			base = pair.Quote
			quote = pair.Base
		}

		decimalsBase, found := p.decimals[base]
		if !found {
			p.logger.Error().
				Str("denom", base).
				Msg("no decimals found")
		}
		decimalsQuote, found := p.decimals[quote]
		if !found {
			p.logger.Error().
				Str("denom", quote).
				Msg("no decimals found")
		}

		price := strToDec(fmt.Sprintf("%v", decoded[0]))

		var diff uint64
		if decimalsBase >= decimalsQuote {
			diff = decimalsBase - decimalsQuote
			price = price.Mul(sdk.NewDec(10).Power(diff))
		} else {
			diff = decimalsQuote - decimalsBase
			price = price.Quo(sdk.NewDec(10).Power(diff))
		}

		now := time.Now()

		p.setTickerPrice(
			symbol,
			price,
			sdk.ZeroDec(),
			now,
		)
	}

	return nil
}

func (p *VelodromeV2Provider) GetAvailablePairs() (map[string]struct{}, error) {
	return p.getAvailablePairsFromContracts()
}

func (p *VelodromeV2Provider) getTokenAddresses(
	contract string,
) ([]string, error) {
	hash, err := keccak256("tokens()")
	if err != nil {
		return nil, err
	}

	data := fmt.Sprintf("%s%064d", hash[:8], 0)

	result, err := p.doEthCall(contract, data)
	if err != nil {
		return nil, err
	}

	types := []string{"address", "address"}

	decoded, err := decodeEthData(result, types)
	if err != nil {
		return nil, err
	}

	addresses := make([]string, 2)
	for i := 0; i < 2; i++ {
		addresses[i] = fmt.Sprintf("%v", decoded[i])
	}

	return addresses, nil
}

func (p *VelodromeV2Provider) setDecimals() {
	p.decimals = map[string]uint64{}
	p.symbols = map[string]string{}

	for _, pair := range p.getAllPairs() {
		contract, err := p.getContractAddress(pair)
		if err != nil {
			p.logger.Error().Err(err)
			continue
		}

		// get base/quote of the contract, getContractAddress also returns
		// the address for the reversed pair
		base := pair.Base
		quote := pair.Quote
		_, found := p.contracts[pair.String()]
		if !found {
			base = pair.Quote
			quote = pair.Base
		}

		denoms := []string{base, quote}

		addresses, err := p.getTokenAddresses(contract)
		if err != nil {
			continue
		}

		for i, address := range addresses {
			denom := denoms[i]
			p.symbols[denom] = address

			_, found = p.decimals[denom]
			if !found {
				p.logger.Info().
					Str("denom", denom).
					Msg("get decimals")
				decimals, err := p.getEthDecimals(address)
				if err != nil {
					p.logger.Error().Err(err)
					continue
				}
				p.decimals[denom] = decimals
			}
		}
	}
}
