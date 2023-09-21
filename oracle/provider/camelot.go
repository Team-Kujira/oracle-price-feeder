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
)

var (
	_ Provider = (*CamelotProvider)(nil)

	camelotV2DefaultEndpoints = Endpoint{
		Name:         ProviderCamelotV2,
		Urls:         []string{"https://api.thegraph.com"},
		PollInterval: 10 * time.Second,
	}

	camelotV3DefaultEndpoints = Endpoint{
		Name:         ProviderCamelotV3,
		Urls:         []string{"https://api.thegraph.com"},
		PollInterval: 10 * time.Second,
	}
)

type (
	// CamelotProvider defines an oracle provider using on chain data from thegraph.com
	//
	// REF: -
	CamelotProvider struct {
		provider
		contracts map[string]string
		volumes   map[string][]CamelotVolume
	}

	CamelotVolume struct {
		Date   int64
		Token0 sdk.Dec
		Token1 sdk.Dec
	}

	CamelotQuery struct {
		Query string `json:"query"`
	}

	CamelotQueryResponse struct {
		Data CamelotResponseData `json:"data"`
	}

	CamelotResponseData struct {
		PairsHourData []CamelotPairHourData `json:"pairHourDatas"`
		Pairs         []CamelotPairData     `json:"pairs"`
		PoolsHourData []CamelotPoolHourData `json:"poolHourDatas"`
		Pools         []CamelotPairData     `json:"pools"`
	}

	CamelotPairHourData struct {
		Time    int64           `json:"hourStartUnix"`
		Pair    CamelotPairData `json:"pair"`
		Volume0 string          `json:"hourlyVolumeToken0"`
		Volume1 string          `json:"hourlyVolumeToken1"`
	}

	CamelotPoolHourData struct {
		Time    int64           `json:"periodStartUnix"`
		Pool    CamelotPairData `json:"pool"`
		Volume0 string          `json:"volumeToken0"`
		Volume1 string          `json:"volumeToken1"`
	}

	// only the quote price is required
	// the inverse price is calculated by
	// the feeder automatically if needed
	CamelotPairData struct {
		Id    string `json:"id"`
		Price string `json:"token1Price"`
	}
)

func NewCamelotProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*CamelotProvider, error) {
	provider := &CamelotProvider{}
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

func (p *CamelotProvider) GetAvailablePairs() (map[string]struct{}, error) {
	return p.getAvailablePairsFromContracts()
}

func (p *CamelotProvider) Poll() error {
	if p.endpoints.Name != ProviderCamelotV2 && p.endpoints.Name != ProviderCamelotV3 {
		return nil
	}

	offset := 3600
	if p.volumes == nil {
		p.volumes = map[string][]CamelotVolume{}
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
		volumes map[string][]CamelotVolume
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

		p.setTickerPrice(
			symbol,
			price,
			volume,
			timestamp,
		)
	}

	return nil
}

func (p *CamelotProvider) query(
	query string,
) (
	prices map[string]sdk.Dec,
	volumes map[string][]CamelotVolume,
	err error,
) {
	var (
		version string
		path    string
	)

	switch p.endpoints.Name {
	case ProviderCamelotV2:
		version = "v2"
		path = "/subgraphs/name/camelotlabs/camelot-amm"
	case ProviderCamelotV3:
		version = "v3"
		path = "/subgraphs/name/camelotlabs/camelot-amm-v3"
	}

	prices = map[string]sdk.Dec{}
	volumes = map[string][]CamelotVolume{}

	request, err := json.Marshal(CamelotQuery{Query: query})
	if err != nil {
		p.logger.Error().Msg("failed marshalling request")
	}

	content, err := p.httpPost(path, request)
	if err != nil {
		return nil, nil, err
	}

	var response CamelotQueryResponse
	err = json.Unmarshal(content, &response)
	if err != nil {
		p.logger.Error().
			Err(err).
			Msg("failed unmarshalling response")
		return nil, nil, err
	}

	if version == "v2" {
		for _, volume := range response.Data.PairsHourData {
			contract := volume.Pair.Id

			_, found := volumes[contract]
			if !found {
				volumes[contract] = []CamelotVolume{}
			}
			volumes[contract] = append(volumes[contract], CamelotVolume{
				Date:   volume.Time,
				Token0: strToDec(volume.Volume0),
				Token1: strToDec(volume.Volume1),
			})
		}

		for _, pair := range response.Data.Pairs {
			prices[pair.Id] = strToDec(pair.Price)
		}
	} else {
		for _, volume := range response.Data.PoolsHourData {
			contract := volume.Pool.Id

			_, found := volumes[contract]
			if !found {
				volumes[contract] = []CamelotVolume{}
			}
			volumes[contract] = append(volumes[contract], CamelotVolume{
				Date:   volume.Time,
				Token0: strToDec(volume.Volume0),
				Token1: strToDec(volume.Volume1),
			})
		}

		for _, pool := range response.Data.Pools {
			prices[pool.Id] = strToDec(pool.Price)
		}
	}

	return prices, volumes, nil
}

func (p *CamelotProvider) getQuery(
	contracts []string,
	offset int,
) (string, error) {
	addresses := `"` + strings.Join(contracts, `","`) + `"`
	timestamp := time.Now().Truncate(1*time.Hour).Unix() - int64(offset)

	query := ""

	switch p.endpoints.Name {
	case ProviderCamelotV2:
		query = fmt.Sprintf(`
		{
			pairHourDatas(
				where: {
					pair_in: [%s],
					hourStartUnix_gte: %d
				}
			) {
				hourStartUnix,
				hourlyVolumeToken0,
				hourlyVolumeToken1,
				pair {
					id
				}
			}
			pairs(
				where: {
					id_in: [%s]
				}
			) {
				id,
				token1Price
			}
		}`,
			addresses,
			timestamp,
			addresses,
		)
	case ProviderCamelotV3:
		query = fmt.Sprintf(`
		{
			poolHourDatas(
				where: {
					pool_in: [%s],
					periodStartUnix_gte: %d
				}
			) {
				periodStartUnix,
				volumeToken0,
				volumeToken1,
				pool {
					id
				}
			}
			pools(
				where: {
					id_in: [%s]
				}
			) {
				id,
				token1Price
			}
		}`,
			addresses,
			timestamp,
			addresses,
		)
	}

	query = strings.ReplaceAll(query, " ", "")
	query = strings.ReplaceAll(query, "\t", "")
	query = strings.ReplaceAll(query, "\n", "")

	return query, nil
}

func (p *CamelotProvider) updateVolumes(volumes map[string][]CamelotVolume) error {
	startHour := time.Now().Truncate(1 * time.Hour).Unix()

	for contract := range volumes {
		_, found := p.volumes[contract]
		if !found {
			p.volumes[contract] = make([]CamelotVolume, 24)
		}

		tmp := map[int64]CamelotVolume{}
		for _, volume := range p.volumes[contract] {
			tmp[volume.Date] = volume
		}

		for _, volume := range volumes[contract] {
			tmp[volume.Date] = volume
		}

		for i := 0; i < 24; i++ {
			timestamp := startHour - int64(i)*3600
			volume := tmp[timestamp]
			p.volumes[contract][i] = CamelotVolume{
				Date:   timestamp,
				Token0: volume.Token0,
				Token1: volume.Token1,
			}
		}
	}

	return nil
}

func (p *CamelotProvider) getVolume(contract string, tokenId int) (sdk.Dec, error) {
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

func (p *CamelotProvider) init() error {
	// lowercase contracts, needed for thegraph api calls
	for symbol, contract := range p.contracts {
		p.contracts[symbol] = strings.ToLower(contract)
	}

	return nil
}
