package provider

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"price-feeder/oracle/provider/volume"
	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
)

var (
	_ Provider = (*CamelotProvider)(nil)

	camelotV2DefaultEndpoints = Endpoint{
		Name:         ProviderCamelotV2,
		Urls:         []string{},
		PollInterval: 15 * time.Second,
		VolumeBlocks: 1,
		VolumePause:  0,
	}

	camelotV3DefaultEndpoints = Endpoint{
		Name:         ProviderCamelotV3,
		Urls:         []string{},
		PollInterval: 15 * time.Second,
		VolumeBlocks: 1,
		VolumePause:  0,
	}
)

type (
	// CamelotProvider defines an oracle provider using on chain data
	CamelotProvider struct {
		provider
		// map topic hash to output types
		// keccak256("Swap(address,address,int256,int256,uint160,uint128,int24)
		// 0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67:
		//   ["int256", "int256", "uint160", "uint128", "int24"]
		topics   map[string][]string
		decimals map[string]uint64
	}
)

func NewCamelotProvider(
	db *sql.DB,
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*CamelotProvider, error) {
	provider := &CamelotProvider{}
	provider.db = db
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	provider.chain = "arbitrum"
	provider.name = "camelotv3"

	provider.topics = map[string][]string{}
	for _, values := range [][]string{{
		"Swap(address,address,int256,int256,uint160,uint128,int24)",
		"int256", "int256", "uint160", "uint128", "int24",
	}} {
		topic, err := keccak256(values[0])
		if err != nil {
			return nil, err
		}
		topic = "0x" + topic
		provider.topics[topic] = values[1:]
	}

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, nil)

	provider.init()

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *CamelotProvider) GetAvailablePairs() (map[string]struct{}, error) {
	return p.getAvailablePairsFromContracts()
}

func (p *CamelotProvider) Poll() error {
	// get latest block
	height, err := p.getEvmHeight()
	if err != nil {
		return err
	}

	if p.height == 0 {
		p.height = height
		return nil
	}

	contracts := []string{}
	for symbol := range p.getAllPairs() {
		contract, found := p.contracts[symbol]
		if !found {
			continue
		}
		contracts = append(contracts, contract)
	}

	// some rpc providers only accept small ranges for getLogs calls
	if p.height+2000 < height {
		p.height = height - 2000
	}

	p.updateVolumes(p.height, height, contracts)

	for i := 0; i < p.endpoints.VolumeBlocks; i++ {
		missing := p.volumes.GetMissing(1)

		if len(missing) == 0 {
			continue
		}

		to := missing[0]

		var from uint64
		if to > 2000 {
			from = to - 2000
		}

		p.updateVolumes(from, to, contracts)
	}

	method := "globalState()"
	types := []string{
		"uint160", "int24", "uint16", "uint16",
		"uint16", "uint8", "uint8", "bool",
	}

	timestamp := time.Now()

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for _, contract := range contracts {
		symbol := p.contracts[contract]

		pair, found := p.getPair(symbol)
		if !found {
			p.logger.Warn().Str("symbol", symbol).Msg("pair not found")
			continue
		}

		decimals1, found := p.decimals[pair.Base]
		if !found {
			p.logger.Warn().Str("symbol", pair.Base).Msg("decimals not found")
		}

		decimals2, found := p.decimals[pair.Quote]
		if !found {
			p.logger.Warn().Str("symbol", pair.Quote).Msg("decimals not found")
		}

		response, err := p.evmCall(contract, method, nil)
		if err != nil {
			return p.error(err)
		}

		var data string
		err = json.Unmarshal(response, &data)
		if err != nil {
			return p.error(err)
		}

		decoded, err := decodeEthData(data, types)
		if err != nil {
			return p.error(err)
		}

		sqrtPrice := fmt.Sprintf("%v", decoded[0])
		price, err := decodeSqrtPrice(sqrtPrice)
		if err != nil {
			return p.error(err)
		}

		factor, err := computeDecimalsFactor(int64(decimals1), int64(decimals2))
		if err != nil {
			return p.error(err)
		}

		price = price.Mul(factor)

		var volume sdk.Dec
		// hack to get the proper volume
		_, found = p.inverse[symbol]
		if found {
			volume, _ = p.volumes.Get(pair.Quote + pair.Base)

			if !volume.IsZero() {
				volume = volume.Quo(price)
			}
		} else {
			volume, _ = p.volumes.Get(pair.String())
		}

		p.setTickerPrice(
			symbol,
			price,
			volume,
			timestamp,
		)
	}

	return nil
}

func (p *CamelotProvider) init() error {
	p.decimals = map[string]uint64{}
	types := []string{"address"}

	for symbol, pair := range p.getAllPairs() {
		logger := p.logger.With().Str("symbol", symbol).Logger()
		logger.Info().Msg("get decimals")

		contract, found := p.contracts[symbol]
		if !found {
			logger.Warn().Msg("contract not found")
			continue
		}

		decimals := make([]uint64, 2)

		for i, method := range []string{"token0()", "token1()"} {
			response, err := p.evmCall(contract, method, nil)
			if err != nil {
				return p.error(err)
			}

			var data string
			err = json.Unmarshal(response, &data)
			if err != nil {
				return p.error(err)
			}

			decoded, err := decodeEthData(data, types)
			if err != nil {
				return p.error(err)
			}
			token := fmt.Sprintf("%v", decoded[0])

			decimals[i], err = p.getEthDecimals(token)
			if err != nil {
				return err
			}
		}

		p.decimals[pair.Base] = decimals[0]
		p.decimals[pair.Quote] = decimals[1]
	}

	return nil
}

func (p *CamelotProvider) updateVolumes(
	height1, height2 uint64,
	addresses []string,
) error {
	if len(p.volumes.Symbols()) == 0 {
		return nil
	}

	if height1 == height2 {
		return nil
	}

	blocks := height2 - height1
	height1 = height1 + 1

	timestamps := make([]time.Time, 2)
	for i, height := range []uint64{height1, height2} {
		block, err := p.evmGetBlockByNumber(height)
		if err != nil {
			return err
		}
		timestamps[i], err = block.GetTime()
		if err != nil {
			return err
		}
	}

	// prepare default values (0) for every symbol

	blocktime := timestamps[1].Sub(timestamps[0]).Seconds() / float64(blocks)
	timestamp := timestamps[0].Unix()

	volumes := make([]volume.Volume, blocks)
	for i := uint64(0); i < blocks; i++ {
		values := map[string]sdk.Dec{}
		for _, symbol := range p.volumes.Symbols() {
			values[symbol] = sdk.ZeroDec()
		}

		volumes[i] = volume.Volume{
			Height: height1 + i,
			Time:   timestamp + int64(float64(i)*blocktime),
			Values: values,
		}
	}

	topics := []string{}
	for topic := range p.topics {
		topics = append(topics, topic)
	}

	logs, err := p.evmGetLogs(height1, height2, addresses, topics)
	if err != nil {
		return err
	}

	for _, log := range logs {
		symbol, found := p.contracts[log.Address]
		if !found {
			p.logger.Warn().Str("contract", log.Address).Msg("symbol not found")
			continue
		}

		pair, found := p.getPair(symbol)
		if !found {
			if !found {
				p.logger.Warn().Str("symbol", symbol).Msg("pair not found")
				continue
			}
		}

		types, found := p.topics[log.Topics[0]]
		if !found {
			err = fmt.Errorf("no types found")
			p.logger.Err(err).
				Str("topic", log.Topics[0]).
				Msg("")
			return err
		}

		data, err := decodeEthData(log.Data, types)
		if err != nil {
			err = fmt.Errorf("failed decoding data")
			p.logger.Err(err).
				Str("topic", log.Topics[0]).
				Msg("")
			return err
		}

		decimals := [2]uint64{}
		for i, denom := range []string{pair.Base, pair.Quote} {
			decimals[i], found = p.decimals[denom]
			if !found {
				p.logger.Warn().Str("denom", denom).Msg("no decimals found")
				continue
			}
		}

		index := int(log.Height - height1)
		if index > len(volumes) || index < 0 {
			err = fmt.Errorf("index < 0 or index > range")
			p.logger.Err(err).
				Uint64("from", height1).
				Uint64("to", height2).
				Uint64("height", log.Height).Msg("")
			return err
		}

		ten := int64ToDec(10)

		symbols := []string{pair.String(), pair.Swap().String()}
		for i, symbol := range symbols {
			current := volumes[index].Values[symbol]
			factor := ten.Power(decimals[i])
			amount := strToDec(fmt.Sprintf("%v", data[i])).Quo(factor).Abs()
			volumes[index].Values[symbol] = current.Add(amount)
		}
	}

	p.volumes.Add(volumes)

	return nil
}
