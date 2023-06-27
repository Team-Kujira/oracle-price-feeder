package provider

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
		ContractAddresses: map[string]string{
			"WSTETHWETH": "0x109830a1AAaD605BbF02a9dFA7B0B92EC2FB7dAa",
		},
	}

	uniswapv3EthCallTemplate = `{
		"jsonrpc": "2.0",
		"method": "eth_call",
		"params": [
			{
				"to": "<contract>", 
				"data": "0x3850c7bd0000000000000000000000000000000000000000000000000000000000000000"
			},
			"latest"
		],
		"id":1
	}`
)

type (
	// UniswapV3Provider defines an oracle provider implemented by the Bitstamp
	// public API.
	//
	// REF: https://www.bitstamp.net/api
	UniswapV3Provider struct {
		provider
		contracts map[string]string
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

		fmt.Println(symbol, contract)

		body := []byte(
			strings.Replace(uniswapv3EthCallTemplate, "<contract>", contract, 1),
		)

		content, err := p.httpPost("", body)
		if err != nil {
			p.logger.Err(err)
			continue
		}

		var response UniswapV3Response
		err = json.Unmarshal(content, &response)
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

		price := sqrtx96.Power(2).Quo(sdk.NewDec(2).Power(192))

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
