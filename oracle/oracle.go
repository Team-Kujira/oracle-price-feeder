package oracle

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"price-feeder/config"
	"price-feeder/oracle/client"
	"price-feeder/oracle/derivative"
	"price-feeder/oracle/history"
	"price-feeder/oracle/provider"
	"price-feeder/oracle/types"
	pfsync "price-feeder/pkg/sync"

	oracletypes "github.com/Team-Kujira/core/x/oracle/types"

	"github.com/cosmos/cosmos-sdk/telemetry"
)

// We define tickerSleep as the minimum timeout between each oracle loop. We
// define this value empirically based on enough time to collect exchange rates,
// and broadcast pre-vote and vote transactions such that they're committed in
// at least one block during each voting period.
const (
	tickerSleep = 1000 * time.Millisecond
)

type ProviderWeight struct {
	Type   string
	Weight map[string]sdk.Dec
}

// PreviousPrevote defines a structure for defining the previous prevote
// submitted on-chain.
type PreviousPrevote struct {
	ExchangeRates     string
	Salt              string
	SubmitBlockHeight int64
}

func NewPreviousPrevote() *PreviousPrevote {
	return &PreviousPrevote{
		Salt:              "",
		ExchangeRates:     "",
		SubmitBlockHeight: 0,
	}
}

// Oracle implements the core component responsible for fetching exchange rates
// for a given set of currency pairs and determining the correct exchange rates
// to submit to the on-chain price oracle adhering the oracle specification.
type Oracle struct {
	logger zerolog.Logger
	closer *pfsync.Closer

	providerTimeout      time.Duration
	providerPairs        map[provider.Name][]types.CurrencyPair
	previousPrevote      *PreviousPrevote
	previousVotePeriod   float64
	priceProviders       map[provider.Name]provider.Provider
	oracleClient         client.OracleClient
	deviations           map[string]sdk.Dec
	providerMinOverrides map[string]int
	endpoints            map[provider.Name]provider.Endpoint
	history              history.PriceHistory
	derivatives          map[string]derivative.Derivative
	derivativePairs      map[string][]types.CurrencyPair
	derivativeSymbols    map[string]struct{}
	contractAddresses    map[string]map[string]string
	providerWeights      map[string]ProviderWeight

	mtx             sync.RWMutex
	lastPriceSyncTS time.Time
	prices          map[string]sdk.Dec
	paramCache      ParamCache
	healthchecks    map[string]http.Client
}

func New(
	logger zerolog.Logger,
	oc client.OracleClient,
	currencyPairs []config.CurrencyPair,
	providerTimeout time.Duration,
	deviations map[string]sdk.Dec,
	providerMinOverrides map[string]int,
	endpoints map[provider.Name]provider.Endpoint,
	derivatives map[string]derivative.Derivative,
	derivativePairs map[string][]types.CurrencyPair,
	derivativeDenoms map[string]struct{},
	healthchecksConfig []config.Healthchecks,
	history history.PriceHistory,
	contractAddresses map[string]map[string]string,
	providerWeights map[string]ProviderWeight,
) *Oracle {
	providerPairs := make(map[provider.Name][]types.CurrencyPair)
	for _, pair := range currencyPairs {
		for _, provider := range pair.Providers {
			providerPairs[provider] = append(providerPairs[provider], types.CurrencyPair{
				Base:  pair.Base,
				Quote: pair.Quote,
			})
		}
	}
	healthchecks := make(map[string]http.Client, len(healthchecksConfig))
	for _, healthcheck := range healthchecksConfig {
		timeout, err := time.ParseDuration(healthcheck.Timeout)
		if err != nil {
			logger.Warn().
				Str("timeout", healthcheck.Timeout).
				Msg("failed to parse healthcheck timeout, skipping configuration")
		} else {
			healthchecks[healthcheck.URL] = http.Client{
				Timeout: timeout,
			}
		}
	}
	return &Oracle{
		logger:               logger.With().Str("module", "oracle").Logger(),
		closer:               pfsync.NewCloser(),
		oracleClient:         oc,
		providerPairs:        providerPairs,
		priceProviders:       make(map[provider.Name]provider.Provider),
		previousPrevote:      nil,
		providerTimeout:      providerTimeout,
		deviations:           deviations,
		providerMinOverrides: providerMinOverrides,
		paramCache:           ParamCache{},
		endpoints:            endpoints,
		healthchecks:         healthchecks,
		derivatives:          derivatives,
		derivativePairs:      derivativePairs,
		derivativeSymbols:    derivativeDenoms,
		history:              history,
		contractAddresses:    contractAddresses,
		providerWeights:      providerWeights,
	}
}

// Start starts the oracle process in a blocking fashion.
func (o *Oracle) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			o.closer.Close()

		default:
			o.logger.Debug().Msg("starting oracle tick")

			startTime := time.Now()

			if err := o.tick(ctx); err != nil {
				telemetry.IncrCounter(1, "failure", "tick")
				o.logger.Err(err).Msg("oracle tick failed")
			}

			o.lastPriceSyncTS = time.Now()

			telemetry.MeasureSince(startTime, "runtime", "tick")
			telemetry.IncrCounter(1, "new", "tick")

			time.Sleep(tickerSleep)
		}
	}
}

// Stop stops the oracle process and waits for it to gracefully exit.
func (o *Oracle) Stop() {
	o.closer.Close()
	<-o.closer.Done()
}

// GetLastPriceSyncTimestamp returns the latest timestamp at which prices where
// fetched from the oracle's set of exchange rate providers.
func (o *Oracle) GetLastPriceSyncTimestamp() time.Time {
	o.mtx.RLock()
	defer o.mtx.RUnlock()

	return o.lastPriceSyncTS
}

// GetPrices returns a copy of the current prices fetched from the oracle's
// set of exchange rate providers.
func (o *Oracle) GetPrices() sdk.DecCoins {
	o.mtx.RLock()
	defer o.mtx.RUnlock()
	// Creates a new array for the prices in the oracle
	prices := sdk.NewDecCoins()
	for k, v := range o.prices {
		// Fills in the prices with each value in the oracle
		prices = prices.Add(sdk.NewDecCoinFromDec(k, v))
	}

	return prices
}

// SetPrices retrieves all the prices and candles from our set of providers as
// determined in the config. If candles are available, uses TVWAP in order
// to determine prices. If candles are not available, uses the most recent prices
// with VWAP. Warns the the user of any missing prices, and filters out any faulty
// providers which do not report prices or candles within 2𝜎 of the others.
func (o *Oracle) SetPrices(ctx context.Context) error {
	g := new(errgroup.Group)
	mtx := new(sync.Mutex)
	requiredRates := make(map[string]struct{})
	providerPrices := provider.AggregatedProviderPrices{}

	for providerName, currencyPairs := range o.providerPairs {
		providerName := providerName
		currencyPairs := currencyPairs

		priceProvider, found := o.priceProviders[providerName]
		if !found {
			endpoint := o.endpoints[providerName]
			contractAddresses, _ := o.contractAddresses[providerName.String()]
			endpoint.ContractAddresses = contractAddresses

			newProvider, err := NewProvider(
				ctx,
				providerName,
				o.logger,
				endpoint,
				o.providerPairs[providerName]...,
			)
			if err != nil {
				return err
			}
			priceProvider = newProvider

			o.priceProviders[providerName] = priceProvider
			continue
		}

		for _, pair := range currencyPairs {
			_, ok := requiredRates[pair.Base]
			if !ok {
				requiredRates[pair.Base] = struct{}{}
			}
		}

		g.Go(func() error {
			prices := make(map[string]types.TickerPrice, 0)
			ch := make(chan struct{})
			errCh := make(chan error, 1)

			go func() {
				var err error
				defer close(ch)
				prices, err = priceProvider.GetTickerPrices(currencyPairs...)
				if err != nil {
					telemetry.IncrCounter(1, "failure", "provider", "type", "ticker")
					errCh <- err
				}
			}()

			select {
			case <-ch:
				break
			case err := <-errCh:
				return err
			case <-time.After(o.providerTimeout):
				telemetry.IncrCounter(1, "failure", "provider", "type", "timeout")
				return fmt.Errorf("provider timed out: %s", providerName)
			}

			// flatten and collect prices based on the base currency per provider
			//
			// e.g.: {ProviderKraken: {"ATOM": <price, volume>, ...}}
			mtx.Lock()
			defer mtx.Unlock()

			filteredPairs := []types.CurrencyPair{}
			for _, pair := range currencyPairs {
				ticker, ok := prices[pair.String()]
				if (!ok || ticker == types.TickerPrice{}) {
					o.logger.Warn().
						Str("pair", pair.String()).
						Str("provider", providerName.String()).
						Msg("no ticker price found")
				} else {
					filteredPairs = append(filteredPairs, pair)
				}
			}

			for _, pair := range filteredPairs {
				ticker := prices[pair.String()]
				_, isDerivative := o.derivativeSymbols[pair.String()]
				if isDerivative {
					err := o.history.AddTickerPrice(pair, providerName.String(), ticker)
					if err != nil {
						o.logger.Error().Err(err).Str("pair", pair.String()).Str("provider", providerName.String()).Msg("failed to add ticker price to history")
					}
				} else {
					_, ok := providerPrices[providerName]
					if !ok {
						providerPrices[providerName] = map[string]types.TickerPrice{}
					}
					providerPrices[providerName][pair.String()] = ticker
				}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		o.logger.Debug().Err(err).Msg("failed to get ticker prices from provider")
	}

	for name, pairs := range o.derivativePairs {
		for _, pair := range pairs {
			symbol := pair.String()
			tickerPrices, err := o.derivatives[name].GetPrices(symbol)
			if err != nil {
				// o.logger.Err(err).Msg("failed to get derivative price")
				continue
			}

			for nameString, tickerPrice := range tickerPrices {
				providerName := provider.Name(nameString)
				_, found := providerPrices[providerName]
				if !found {
					providerPrices[providerName] = map[string]types.TickerPrice{}
				}
				providerPrices[providerName][symbol] = tickerPrice

				provider.TelemetryProviderPrice(
					provider.Name(nameString+"_twap"),
					symbol,
					float32(tickerPrice.Price.MustFloat64()),
					float32(tickerPrice.Volume.MustFloat64()),
				)
			}
		}
	}

	computedPrices, err := GetComputedPrices(
		o.logger,
		providerPrices,
		o.providerPairs,
		o.deviations,
		o.providerMinOverrides,
		o.providerWeights,
	)
	if err != nil {
		return err
	}

	if len(computedPrices) != len(requiredRates) {
		missingPrices := []string{}
		for base := range requiredRates {
			if _, ok := computedPrices[base]; !ok {
				missingPrices = append(missingPrices, base)
			}
		}

		sort.Strings(missingPrices)
		o.logger.Error().Msg(
			"unable to get prices for: " + strings.Join(missingPrices, ", "),
		)
	}

	o.prices = computedPrices

	return nil
}

// GetComputedPrices gets the candle and ticker prices and computes it.
// It returns candles' TVWAP if possible, if not possible (not available
// or due to some staleness) it will use the most recent ticker prices
// and the VWAP formula instead.
func GetComputedPrices(
	logger zerolog.Logger,
	providerPrices provider.AggregatedProviderPrices,
	providerPairs map[provider.Name][]types.CurrencyPair,
	deviations map[string]sdk.Dec,
	providerMinOverrides map[string]int,
	providerWeights map[string]ProviderWeight,
) (prices map[string]sdk.Dec, err error) {
	rates, err := convertTickersToUSD(
		logger,
		providerPrices,
		providerPairs,
		deviations,
		providerMinOverrides,
		providerWeights,
	)
	if err != nil {
		return nil, err
	}

	return rates, nil
}

// GetParamCache returns the last updated parameters of the x/oracle module
// if the current ParamCache is outdated, we will query it again.
func (o *Oracle) GetParamCache(ctx context.Context, currentBlockHeigh int64) (oracletypes.Params, error) {
	if !o.paramCache.IsOutdated(currentBlockHeigh) {
		return *o.paramCache.params, nil
	}

	params, err := o.GetParams(ctx)
	if err != nil {
		return oracletypes.Params{}, err
	}

	o.checkWhitelist(params)
	o.paramCache.Update(currentBlockHeigh, params)
	return params, nil
}

// GetParams returns the current on-chain parameters of the x/oracle module.
func (o *Oracle) GetParams(ctx context.Context) (oracletypes.Params, error) {
	grpcConn, err := grpc.Dial(
		o.oracleClient.GRPCEndpoint,
		// the Cosmos SDK doesn't support any transport security mechanism
		grpc.WithInsecure(),
		grpc.WithContextDialer(dialerFunc),
	)
	if err != nil {
		return oracletypes.Params{}, fmt.Errorf("failed to dial Cosmos gRPC service: %w", err)
	}

	defer grpcConn.Close()
	queryClient := oracletypes.NewQueryClient(grpcConn)

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	queryResponse, err := queryClient.Params(ctx, &oracletypes.QueryParamsRequest{})
	if err != nil {
		return oracletypes.Params{}, fmt.Errorf("failed to get x/oracle params: %w", err)
	}

	return queryResponse.Params, nil
}

func NewProvider(
	ctx context.Context,
	providerName provider.Name,
	logger zerolog.Logger,
	endpoint provider.Endpoint,
	providerPairs ...types.CurrencyPair,
) (provider.Provider, error) {
	endpoint.Name = providerName
	providerLogger := logger.With().Str("provider", providerName.String()).Logger()
	switch providerName {

	case
		provider.ProviderAstroportNeutron,
		provider.ProviderAstroportTerra2,
		provider.ProviderAstroportInjective:
		return provider.NewAstroportProvider(ctx, providerLogger, endpoint, providerPairs...)
	case
		provider.ProviderBinance,
		provider.ProviderBinanceUS:
		return provider.NewBinanceProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderBitfinex:
		return provider.NewBitfinexProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderBitget:
		return provider.NewBitgetProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderBitstamp:
		return provider.NewBitstampProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderBkex:
		return provider.NewBkexProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderBitmart:
		return provider.NewBitmartProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderBybit:
		return provider.NewBybitProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderCamelotV2, provider.ProviderCamelotV3:
		return provider.NewCamelotProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderCoinbase:
		return provider.NewCoinbaseProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderCrypto:
		return provider.NewCryptoProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderCurve:
		return provider.NewCurveProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderFin:
		return provider.NewFinProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderFinV2:
		return provider.NewFinV2Provider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderGate:
		return provider.NewGateProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderHitBtc:
		return provider.NewHitBtcProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderHuobi:
		return provider.NewHuobiProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderIdxOsmosis:
		return provider.NewIdxProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderKraken:
		return provider.NewKrakenProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderKucoin:
		return provider.NewKucoinProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderLbank:
		return provider.NewLbankProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderMexc:
		return provider.NewMexcProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderMock:
		return provider.NewMockProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderOkx:
		return provider.NewOkxProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderOsmosis:
		return provider.NewOsmosisProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderOsmosisV2:
		return provider.NewOsmosisV2Provider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderPancakeV3Bsc:
		return provider.NewPancakeProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderPhemex:
		return provider.NewPhemexProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderPoloniex:
		return provider.NewPoloniexProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderPyth:
		return provider.NewPythProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderUniswapV3:
		return provider.NewUniswapV3Provider(ctx, providerLogger, endpoint, providerPairs...)
	case
		provider.ProviderWhitewhaleCmdx,
		provider.ProviderWhitewhaleHuahua,
		provider.ProviderWhitewhaleInj,
		provider.ProviderWhitewhaleJuno,
		provider.ProviderWhitewhaleLunc,
		provider.ProviderWhitewhaleLuna,
		provider.ProviderWhitewhaleSei,
		provider.ProviderWhitewhaleWhale:
		return provider.NewWhitewhaleProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderXt:
		return provider.NewXtProvider(ctx, providerLogger, endpoint, providerPairs...)
	case provider.ProviderZero:
		return provider.NewZeroProvider(ctx, providerLogger, endpoint, providerPairs...)

	}
	return nil, fmt.Errorf("provider %s not found", providerName)
}

func (o *Oracle) checkWhitelist(params oracletypes.Params) {
	for _, denom := range params.Whitelist {
		symbol := strings.ToUpper(denom.Name)
		if _, ok := o.prices[symbol]; !ok {
			o.logger.Warn().Str("denom", symbol).Msg("price missing for required denom")
		}
	}
}

func (o *Oracle) tick(ctx context.Context) error {
	o.logger.Info().Msg("executing oracle tick")

	// Create and start all provider routines immediately
	if len(o.priceProviders) == 0 {
		o.SetPrices(ctx)
	}

	blockHeight, err := o.oracleClient.ChainHeight.GetChainHeight()
	if err != nil {
		return err
	}
	if blockHeight < 1 {
		return fmt.Errorf("expected positive block height")
	}

	oracleParams, err := o.GetParamCache(ctx, blockHeight)
	if err != nil {
		return err
	}

	// Get oracle vote period, next block height, current vote period, and index
	// in the vote period.
	oracleVotePeriod := int64(oracleParams.VotePeriod)
	nextBlockHeight := blockHeight + 1
	currentVotePeriod := math.Floor(float64(nextBlockHeight) / float64(oracleVotePeriod))
	indexInVotePeriod := nextBlockHeight % oracleVotePeriod

	o.logger.Debug().
		Int64("vote_period", oracleVotePeriod).
		Float64("previous_vote_period", o.previousVotePeriod).
		Float64("current_vote_period", currentVotePeriod).
		Int64("indexInVotePeriod", indexInVotePeriod).
		Msg("")

	// Skip until new voting period. Specifically, skip when:
	// index [0, oracleVotePeriod - 1] > oracleVotePeriod - 2 OR index is 0
	if (o.previousVotePeriod != 0 && currentVotePeriod == o.previousVotePeriod) ||
		(indexInVotePeriod > 0 && oracleVotePeriod-indexInVotePeriod > 4) {
		// oracleVotePeriod-indexInVotePeriod < 2 || (indexInVotePeriod > 0 && indexInVotePeriod < int64(float64(oracleVotePeriod)*0.75)) {
		o.logger.Info().
			Msg("skipping until next voting period")

		return nil
	}

	if err := o.SetPrices(ctx); err != nil {
		return err
	}

	// If we're past the voting period we needed to hit, reset and submit another
	// prevote.
	if o.previousVotePeriod != 0 && currentVotePeriod-o.previousVotePeriod != 1 {
		o.logger.Info().
			Msg("missing vote during voting period")
		telemetry.IncrCounter(1, "vote", "failure", "missed")

		o.previousVotePeriod = 0
		o.previousPrevote = nil
		return nil
	}

	salt, err := GenerateSalt(32)
	if err != nil {
		return err
	}

	valAddr, err := sdk.ValAddressFromBech32(o.oracleClient.ValidatorAddrString)
	if err != nil {
		return err
	}

	exchangeRatesStr := GenerateExchangeRatesString(o.GetPrices())
	hash := oracletypes.GetAggregateVoteHash(salt, exchangeRatesStr, valAddr)
	preVoteMsg := &oracletypes.MsgAggregateExchangeRatePrevote{
		Hash:      hash.String(), // hash of prices from the oracle
		Feeder:    o.oracleClient.OracleAddrString,
		Validator: valAddr.String(),
	}

	isPrevoteOnlyTx := o.previousPrevote == nil
	if isPrevoteOnlyTx {
		// This timeout could be as small as oracleVotePeriod-indexInVotePeriod,
		// but we give it some extra time just in case.
		//
		// Ref : https://github.com/terra-money/oracle-feeder/blob/baef2a4a02f57a2ffeaa207932b2e03d7fb0fb25/feeder/src/vote.ts#L222
		o.logger.Info().
			Str("hash", hash.String()).
			Str("validator", preVoteMsg.Validator).
			Str("feeder", preVoteMsg.Feeder).
			Msg("broadcasting pre-vote")
		if err := o.oracleClient.BroadcastTx(nextBlockHeight, oracleVotePeriod*2, preVoteMsg); err != nil {
			return err
		}

		currentHeight, err := o.oracleClient.ChainHeight.GetChainHeight()
		if err != nil {
			return err
		}

		o.previousVotePeriod = math.Floor(float64(currentHeight) / float64(oracleVotePeriod))
		o.previousPrevote = &PreviousPrevote{
			Salt:              salt,
			ExchangeRates:     exchangeRatesStr,
			SubmitBlockHeight: currentHeight,
		}
	} else {
		// otherwise, we're in the next voting period and thus we vote
		voteMsg := &oracletypes.MsgAggregateExchangeRateVote{
			Salt:          o.previousPrevote.Salt,
			ExchangeRates: o.previousPrevote.ExchangeRates,
			Feeder:        o.oracleClient.OracleAddrString,
			Validator:     valAddr.String(),
		}

		o.logger.Info().
			Str("exchange_rates", voteMsg.ExchangeRates).
			Str("validator", voteMsg.Validator).
			Str("feeder", voteMsg.Feeder).
			Msg("broadcasting vote")
		if err := o.oracleClient.BroadcastTx(
			nextBlockHeight,
			oracleVotePeriod-indexInVotePeriod,
			voteMsg,
		); err != nil {
			return err
		}

		o.previousPrevote = nil
		o.previousVotePeriod = 0
		o.healthchecksPing()
	}

	return nil
}

func (o *Oracle) healthchecksPing() {
	for url, client := range o.healthchecks {
		o.logger.Info().Msg("updating healthcheck status")
		_, err := client.Get(url)
		if err != nil {
			o.logger.Warn().Msg("healthcheck ping failed")
		}
	}
}

// GenerateSalt generates a random salt, size length/2,  as a HEX encoded string.
func GenerateSalt(length int) (string, error) {
	if length == 0 {
		return "", fmt.Errorf("failed to generate salt: zero length")
	}

	bytes := make([]byte, length)

	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}

	return hex.EncodeToString(bytes), nil
}

// GenerateExchangeRatesString generates a canonical string representation of
// the aggregated exchange rates.
func GenerateExchangeRatesString(prices sdk.DecCoins) string {
	prices.Sort()
	return prices.String()
}
