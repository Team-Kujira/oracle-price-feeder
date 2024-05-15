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
	_ Provider = (*FinV2Provider)(nil)

	finV2DefaultEndpoints = Endpoint{
		Name:         ProviderFinV2,
		Urls:         []string{},
		PollInterval: 3 * time.Second,
		VolumeBlocks: 4,
		VolumePause:  0,
	}
)

type (
	// FinV2 defines an oracle provider that uses the API of an Kujira node
	// to directly retrieve the price from the fin contract
	FinV2Provider struct {
		provider
		contracts map[string]string
		delta     map[string]int64
		volumes   volume.VolumeHandler
		height    uint64
		decimals  map[string]int64
	}

	FinV2BookResponse struct {
		Data FinV2BookData `json:"data"`
	}

	FinV2BookData struct {
		Base  []FinV2Order `json:"base"`
		Quote []FinV2Order `json:"quote"`
	}

	FinV2Order struct {
		Price string `json:"quote_price"`
	}

	FinV2ConfigResponse struct {
		Data FinV2Config `json:"data"`
	}

	FinV2Config struct {
		Delta int64 `json:"decimal_delta"`
	}
)

func NewFinV2Provider(
	db *sql.DB,
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*FinV2Provider, error) {
	provider := &FinV2Provider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	provider.contracts = provider.endpoints.ContractAddresses

	for symbol, contract := range provider.endpoints.ContractAddresses {
		provider.contracts[contract] = symbol
	}

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, nil)

	provider.delta = map[string]int64{}

	volumes, err := volume.NewVolumeHandler(logger, db, "finv2", pairs, 60*60*24)
	if err != nil {
		return provider, err
	}

	provider.volumes = volumes

	provider.decimals = map[string]int64{
		"KUJI": 6,
		"USDC": 6,
		"USK":  6,
		"MNTA": 6,
		"ATOM": 6,
	}

	go startPolling(provider, provider.endpoints.PollInterval, logger)

	return provider, nil
}

func (p *FinV2Provider) Poll() error {
	p.updateVolumes()

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

		content, err := p.wasmSmartQuery(contract, `{"book":{"limit":1}}`)
		if err != nil {
			return err
		}

		var bookResponse FinV2BookResponse
		err = json.Unmarshal(content, &bookResponse)
		if err != nil {
			return err
		}

		if len(bookResponse.Data.Base) < 1 || len(bookResponse.Data.Quote) < 1 {
			return fmt.Errorf("no order found")
		}

		base := strToDec(bookResponse.Data.Base[0].Price)
		quote := strToDec(bookResponse.Data.Quote[0].Price)

		var low, high sdk.Dec

		if base.LT(quote) {
			low = base
			high = quote
		} else {
			low = quote
			high = base
		}

		if high.GT(low.Mul(floatToDec(1.1))) {
			spread := high.Sub(low).Quo(low)
			p.logger.Error().
				Str("spread", spread.String()).
				Str("symbol", symbol).
				Msg("spread too large")
			continue
		}

		delta, err := p.getDecimalDelta(contract)
		if err != nil {
			continue
		}

		price := base.Add(quote).QuoInt64(2)
		if delta < 0 {
			price = price.Quo(uintToDec(10).Power(uint64(delta * -1)))
		} else {
			price = price.Mul(uintToDec(10).Power(uint64(delta)))
		}

		var volume sdk.Dec
		// hack to get the proper volume
		_, found := p.inverse[symbol]
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

func (p *FinV2Provider) GetAvailablePairs() (map[string]struct{}, error) {
	return p.getAvailablePairsFromContracts()
}

func (p *FinV2Provider) getDecimalDelta(contract string) (int64, error) {
	delta, found := p.delta[contract]
	if found {
		return delta, nil
	}

	content, err := p.wasmSmartQuery(contract, `{"config":{}}`)
	if err != nil {
		return 0, err
	}

	var response FinV2ConfigResponse

	err = json.Unmarshal(content, &response)
	if err != nil {
		p.logger.Err(err).Msg("")
		return 0, nil
	}

	delta = response.Data.Delta

	p.delta[contract] = delta

	return delta, nil
}

func (p *FinV2Provider) updateVolumes() {
	missing := p.volumes.GetMissing(p.endpoints.VolumeBlocks)
	missing = append(missing, 0)

	volumes := make([]volume.Volume, len(missing))

	for i, height := range missing {
		volume, err := p.getVolume(height)
		time.Sleep(time.Millisecond * time.Duration(p.endpoints.VolumePause))
		if err != nil {
			p.error(err)
			continue
		}
		volumes[i] = volume
	}

	p.volumes.Add(volumes)
	p.volumes.Debug("KUJIUSDC")
}

func (p *FinV2Provider) getVolume(height uint64) (volume.Volume, error) {
	p.logger.Info().Uint64("height", height).Msg("get volume")

	var err error

	type Denom struct {
		Symbol   string
		Decimals int64
		Amount   sdk.Dec
	}

	if height == 0 {
		height, err = p.getCosmosHeight()
		if err != nil {
			return volume.Volume{}, p.error(err)
		}

		if height == p.height || height == 0 {
			return volume.Volume{}, nil
		}

		p.height = height
	}

	// prepare all volumes:
	// not traded pairs have zero volume for this block
	values := map[string]sdk.Dec{}

	for _, pair := range p.getAllPairs() {
		values[pair.Base+pair.Quote] = sdk.ZeroDec()
		values[pair.Quote+pair.Base] = sdk.ZeroDec()
	}

	txs, timestamp, err := p.getCosmosTxs(height)
	if err != nil {
		return volume.Volume{}, p.error(err)
	}

	for _, tx := range txs {
		trades := tx.GetEventsByType("wasm-trade")
		for _, event := range trades {
			contract, found := event.Attributes["_contract_address"]
			if !found {
				continue
			}

			symbol, found := p.contracts[contract]
			if !found {
				continue
			}

			pair, err := p.getPair(symbol)
			if err != nil {
				p.logger.Warn().Err(err).Str("symbol", symbol).Msg("")
				continue
			}

			base := Denom{
				Symbol:   pair.Base,
				Decimals: p.decimals[pair.Base],
				Amount:   strToDec(event.Attributes["base_amount"]),
			}

			quote := Denom{
				Symbol:   pair.Quote,
				Decimals: p.decimals[pair.Quote],
				Amount:   strToDec(event.Attributes["quote_amount"]),
			}

			if base.Decimals == 0 && quote.Decimals == 0 {
				p.logger.Error().
					Str("pair", pair.String()).
					Msg("no decimals found")
				continue
			}

			if base.Decimals == 0 || quote.Decimals == 0 {
				delta, err := p.getDecimalDelta(contract)
				if err != nil {
					p.logger.Err(err).Msg("")
					continue
				}

				if base.Decimals == 0 {
					base.Decimals = quote.Decimals + delta
				} else {
					quote.Decimals = base.Decimals - delta
				}
			}

			ten := uintToDec(10)

			base.Amount = base.Amount.Quo(ten.Power(uint64(base.Decimals)))
			quote.Amount = quote.Amount.Quo(ten.Power(uint64(quote.Decimals)))

			// needed to for final volumes: {KUJIUSK: 1, USKKUJI: 2}
			denoms := map[string]Denom{
				pair.Base + pair.Quote: base,
				pair.Quote + pair.Base: quote,
			}

			for symbol, denom := range denoms {
				volume, found := values[symbol]
				if !found {
					p.logger.Error().
						Str("symbol", symbol).
						Msg("volume not set")
					continue
				}

				values[symbol] = volume.Add(denom.Amount)
			}
		}
	}

	volume := volume.Volume{
		Height: height,
		Time:   timestamp.Unix(),
		Values: values,
	}

	return volume, nil
}
