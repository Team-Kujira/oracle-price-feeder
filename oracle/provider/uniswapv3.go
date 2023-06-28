package provider

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"

	"github.com/ethereum/go-ethereum/accounts/abi"
)

var (
	_                         Provider = (*UniswapV3Provider)(nil)
	uniswapv3DefaultEndpoints          = Endpoint{
		Name:         ProviderUniswapV3,
		Urls:         []string{"https://eth.llamarpc.com", "https://ethereum.publicnode.com"},
		PollInterval: 10 * time.Second,
		// ContractAddresses: map[string]string{
		// 	"WSTETHWETH": "0x109830a1AAaD605BbF02a9dFA7B0B92EC2FB7dAa",
		// },
	}
)

type (
	// UniswapV3Provider defines an oracle provider calling uniswap pools
	// directly on ethereum
	UniswapV3Provider struct {
		provider
		decimals map[string]uint64
	}

	UniswapV3Response struct {
		Result string `json:"result"` // Encoded data ex.: 0x0000000000000...
	}
)

func NewUniswapV3Provider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*UniswapV3Provider, error) {
	provider := &UniswapV3Provider{}
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

func (p *UniswapV3Provider) Poll() error {
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

		decoded, err := decodeEthData(response.Result, types)
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

func (p *UniswapV3Provider) GetAvailablePairs() (map[string]struct{}, error) {
	return p.getAvailablePairsFromContracts()
}

// token0() 0dfe1681
// token1() d21220a7
// decimals() 313ce567

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
		fmt.Println(err)
		return nil, err
	}

	return abi.Unpack("fn", decoded)

}

func (p *UniswapV3Provider) doEthCall(address string, data string) (UniswapV3Response, error) {
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

	body := Body{
		Jsonrpc: "2.0",
		Method:  "eth_call",
		Params: []interface{}{
			Transaction{
				To:   address,
				Data: "0x" + data,
			},
			"latest",
		},
		Id: 1,
	}

	bz, err := json.Marshal(body)
	if err != nil {
		return UniswapV3Response{}, err
	}

	content, err := p.httpPost("", bz)
	if err != nil {
		return UniswapV3Response{}, err
	}

	var response UniswapV3Response
	err = json.Unmarshal(content, &response)
	if err != nil {
		return UniswapV3Response{}, err
	}

	return response, nil
}

func (p *UniswapV3Provider) getUniswapV3Token(contract string, index int64) (string, error) {
	if index < 0 || index > 1 {
		return "", fmt.Errorf("index must be either 0 or 1")
	}

	hash := []string{"0dfe1681", "d21220a7"}[index]
	data := fmt.Sprintf("%s%064d", hash, 0)

	response, err := p.doEthCall(contract, data)
	if err != nil {
		return "", err
	}

	types := []string{"address"}

	decoded, err := decodeEthData(response.Result, types)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%v", decoded[0]), nil
}

func (p *UniswapV3Provider) getUniswapV3Decimals(contract string) (uint64, error) {
	data := fmt.Sprintf("313ce567%064d", 0)

	response, err := p.doEthCall(contract, data)
	if err != nil {
		return 0, err
	}

	types := []string{"uint8"}

	decoded, err := decodeEthData(response.Result, types)
	if err != nil {
		return 0, err
	}

	decimals, err := strconv.ParseUint(fmt.Sprintf("%v", decoded[0]), 10, 8)
	if err != nil {
		return 0, err
	}

	return decimals, nil
}

func (p *UniswapV3Provider) setDecimals() {
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
			tokenAddress, err := p.getUniswapV3Token(contract, i)
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
				decimals, err := p.getUniswapV3Decimals(tokenAddress)
				if err != nil {
					p.logger.Error().Err(err)
					continue
				}
				p.decimals[denom] = decimals
			}
		}
	}
	fmt.Println(p.decimals)
}
