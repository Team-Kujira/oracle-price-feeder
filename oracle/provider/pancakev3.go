package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
	"github.com/shopspring/decimal"
)

var (
	_ Provider = (*PancakeProvider)(nil)

	PancakeV3BscDefaultEndpoints = Endpoint{
		Name:         ProviderPancakeV3Bsc,
		Urls:         []string{"https://api.thegraph.com"},
		PollInterval: 10 * time.Second,
	}
)

type (
	// PancakeProvider defines an oracle provider using on chain data from thegraph.com
	//
	// REF: -
	PancakeProvider struct {
		provider
		contracts map[string]string
		volumes   map[string][]PancakeVolume
	}

	PancakeVolume struct {
		Date   int64
		Token0 sdk.Dec
		Token1 sdk.Dec
	}

	PancakeQuery struct {
		Query string `json:"query"`
	}

	PancakeQueryResponse struct {
		Data PancakeResponseData `json:"data"`
	}

	PancakeResponseData struct {
		Pools []PancakePool `json:"pools"`
	}

	PancakePool struct {
		Id        string            `json:"id"`
		Token0    PancakeToken      `json:"token0"`
		Token1    PancakeToken      `json:"token1"`
		SqrtPrice string            `json:"sqrtPrice"`
		HourData  []PancakeHourData `json:"poolHourData"`
	}

	PancakeToken struct {
		Id string `json:"id"`
	}

	PancakeHourData struct {
		Time    int64  `json:"periodStartUnix"`
		Volume0 string `json:"volumeToken0"`
		Volume1 string `json:"volumeToken1"`
	}
)

func NewPancakeProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*PancakeProvider, error) {
	provider := &PancakeProvider{}
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

	provider.init()

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *PancakeProvider) GetAvailablePairs() (map[string]struct{}, error) {
	return p.getAvailablePairsFromContracts()
}

func (p *PancakeProvider) Poll() error {
	if p.endpoints.Name != ProviderPancakeV3Bsc {
		return nil
	}

	offset := 3600
	if p.volumes == nil {
		p.volumes = map[string][]PancakeVolume{}
		offset = 23 * 3600
	}

	contracts := []string{}

	for _, pair := range p.getAllPairs() {

		contract, err := p.getContractAddress(pair)
		if err != nil {
			continue
		}

		contracts = append(contracts, contract)
	}

	query, _ := p.getQuery(contracts, offset)

	var (
		prices  map[string]sdk.Dec
		volumes map[string][]PancakeVolume
	)

	prices, volumes, _ = p.query(query)

	p.updateVolumes(volumes)

	timestamp := time.Now()

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for symbol, pair := range p.getAllPairs() {
		contract, err := p.getContractAddress(pair)
		if err != nil {
			p.logger.Warn().
				Str("symbol", symbol).
				Msg("no pool id found")
			continue
		}

		price, found := prices[contract]
		if !found {
			continue
		}

		var tokenId int
		_, found = p.inverse[symbol]
		if found {
			tokenId = 1
		} else {
			tokenId = 0
		}

		volume, err := p.getVolume(contract, tokenId)
		if err != nil {
			p.logger.Error().Msg("failed getting volume data")
		}

		if tokenId == 1 {
			// setTickerPrice() does the reverse calculation by default
			// so we need to provide a prepared volume
			volume = volume.Quo(price)
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

func (p *PancakeProvider) query(
	query string,
) (
	prices map[string]sdk.Dec,
	volumes map[string][]PancakeVolume,
	err error,
) {
	// version string
	var path string

	switch p.endpoints.Name {
	case ProviderPancakeV3Bsc:
		// version = "v3"
		path = "/subgraphs/name/pancakeswap/exchange-v3-bsc"
	}

	prices = map[string]sdk.Dec{}
	volumes = map[string][]PancakeVolume{}

	request, err := json.Marshal(CamelotQuery{Query: query})
	if err != nil {
		p.logger.Error().Msg("failed marshalling request")
	}

	content, err := p.httpPost(path, request)
	if err != nil {
		return nil, nil, err
	}

	var response PancakeQueryResponse
	err = json.Unmarshal(content, &response)
	if err != nil {
		p.logger.Error().
			Err(err).
			Msg("failed unmarshalling response")
		return nil, nil, err
	}

	for _, pool := range response.Data.Pools {
		contract := pool.Id

		_, found := volumes[contract]
		if !found {
			volumes[contract] = []PancakeVolume{}
		}

		for _, volume := range pool.HourData {
			volumes[contract] = append(volumes[contract], PancakeVolume{
				Date:   volume.Time,
				Token0: strToDec(volume.Volume0),
				Token1: strToDec(volume.Volume1),
			})
		}
	}

	for _, pool := range response.Data.Pools {
		dec, err := decimal.NewFromString(pool.SqrtPrice)
		if err != nil {
			p.logger.Error().
				Err(err).
				Msg("failed parsing sqrt price")
			return nil, nil, err
		}

		d2 := decimal.NewFromInt(2)
		d192 := decimal.NewFromInt(192)

		dec = dec.Pow(d2).Div(d2.Pow(d192))
		price := strToDec(dec.String())

		prices[pool.Id] = price
	}

	return prices, volumes, nil
}

func (p *PancakeProvider) getQuery(
	contracts []string,
	offset int,
) (string, error) {
	addresses := `"` + strings.Join(contracts, `","`) + `"`
	timestamp := time.Now().Truncate(1*time.Hour).Unix() - int64(offset)

	query := ""

	switch p.endpoints.Name {
	case ProviderPancakeV3Bsc:
		query = fmt.Sprintf(`
		{
			pools(
				where:{
			  		id_in: [%s]
				}
			) {
			  	id,
				sqrtPrice,
			  	poolHourData(
					where: {
						periodStartUnix_gte: %d
			  		}
				) {
					periodStartUnix,
					volumeToken0,
					volumeToken1
			  	}
				}
		}`,
			addresses,
			timestamp,
		)
	}

	query = strings.ReplaceAll(query, " ", "")
	query = strings.ReplaceAll(query, "\t", "")
	query = strings.ReplaceAll(query, "\n", "")

	return query, nil
}

func (p *PancakeProvider) updateVolumes(volumes map[string][]PancakeVolume) error {
	startHour := time.Now().Truncate(1 * time.Hour).Unix()

	for contract := range volumes {
		_, found := p.volumes[contract]
		if !found {
			p.volumes[contract] = make([]PancakeVolume, 24)
		}

		tmp := map[int64]PancakeVolume{}
		for _, volume := range p.volumes[contract] {
			tmp[volume.Date] = volume
		}

		for _, volume := range volumes[contract] {
			tmp[volume.Date] = volume
		}

		for i := 0; i < 24; i++ {
			timestamp := startHour - int64(i)*3600
			volume := tmp[timestamp]
			p.volumes[contract][i] = PancakeVolume{
				Date:   timestamp,
				Token0: volume.Token0,
				Token1: volume.Token1,
			}
		}
	}

	return nil
}

func (p *PancakeProvider) getVolume(contract string, tokenId int) (sdk.Dec, error) {
	value := sdk.NewDec(0)
	volumes, found := p.volumes[contract]
	if !found {
		msg := "volume data not found"
		p.logger.Error().
			Str("contract", contract).
			Msg(msg)
		return value, fmt.Errorf(msg)
	}

	for _, volume := range volumes {
		var v sdk.Dec

		if tokenId == 0 {
			v = volume.Token0
		} else {
			v = volume.Token1
		}

		if v.IsNil() {
			continue
		}

		value = value.Add(v)
	}

	return value, nil
}

func (p *PancakeProvider) init() error {
	// lowercase contracts, needed for thegraph api calls
	for symbol, contract := range p.contracts {
		p.contracts[symbol] = strings.ToLower(contract)
	}

	return nil
}
