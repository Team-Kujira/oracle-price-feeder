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
		Name: ProviderUniswapV3,
		Urls: []string{
			"https://optimism.llamarpc.com",
		},
		PollInterval: 10 * time.Second,
		// ContractAddresses: map[string]string{
		// 	"WSTETHWETH": "0x109830a1AAaD605BbF02a9dFA7B0B92EC2FB7dAa",
		// },
	}
)

type (
	// VelodromeV2Provider defines an oracle provider calling velodrome pools
	// directly on optimism
	VelodromeV2Provider struct {
		provider
		decimals map[string]uint64
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
	types := []string{
		"uint160", "int24", "uint16", "uint16", "uint16", "uint8", "bool",
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

		data := fmt.Sprintf("3850c7bd%064d", 0)
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

		sqrtx96 := strToDec(fmt.Sprintf("%v", decoded[0]))

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

		price := sqrtx96.Power(2).Quo(sdk.NewDec(2).Power(192))

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

// token0() 0dfe1681
// token1() d21220a7
// decimals() 313ce567

func (p *VelodromeV2Provider) getTokenAddress(
	contract string,
	index int64,
) (string, error) {
	if index < 0 || index > 1 {
		return "", fmt.Errorf("index must be either 0 or 1")
	}

	hash, err := keccak256(fmt.Sprintf("token%d", index))
	if err != nil {
		return "", err
	}

	data := fmt.Sprintf("%s%064d", hash, 0)

	result, err := p.doEthCall(contract, data)
	if err != nil {
		return "", err
	}

	types := []string{"address"}

	decoded, err := decodeEthData(result, types)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%v", decoded[0]), nil
}

func (p *VelodromeV2Provider) setDecimals() {
	p.decimals = map[string]uint64{}

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

		// get decimals for token0 and token1
		for i := int64(0); i < 2; i++ {
			tokenAddress, err := p.getTokenAddress(contract, i)
			if err != nil {
				p.logger.Error().Err(err)
				continue
			}

			denom := denoms[i]

			_, found = p.decimals[denom]
			if !found {
				p.logger.Info().
					Str("denom", denom).
					Msg("get decimals")
				decimals, err := p.getEthDecimals(tokenAddress)
				if err != nil {
					p.logger.Error().Err(err)
					continue
				}
				p.decimals[denom] = decimals
			}
		}
	}
}
