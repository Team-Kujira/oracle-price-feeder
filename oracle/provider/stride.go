package provider

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
)

const (
	strideRestURL = "https://stride-fleet.main.stridenet.co/"
)

var _ Provider = (*StrideProvider)(nil)

type (
	StrideProvider struct {
		baseURL string
	}

	StrideRestHostZoneResponse struct {
		HostZones []StrideHostZone `json:"host_zone"`
	}

	StrideHostZone struct {
		ChainID        string `json:"chain_id"`
		HostDenom      string `json:"host_denom"`
		RedemptionRate string `json:"redemption_rate"`
	}
)

func NewStrideProvider(endpoint Endpoint) *StrideProvider {
	if endpoint.Name == ProviderStride {
		return &StrideProvider{
			baseURL: endpoint.Rest,
		}
	}
	return &StrideProvider{
		baseURL: strideRestURL,
	}
}

func (p *StrideProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	return types.TickerPrice{}, nil
}

func (p *StrideProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	fmt.Println(cps)

	resp, err := http.Get(p.baseURL + "/api/Stride-Labs/stride/stakeibc/host_zone")
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	bz, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read host zone response body: %w", err)
	}

	var hostZoneResp StrideRestHostZoneResponse
	err = json.Unmarshal(bz, &hostZoneResp)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal host zone response body: %w", err)
	}

	hostDenoms := make(map[string]string)
	for _, cp := range cps {
		hostDenoms["u"+strings.ToLower(cp.Quote)] = cp.String()
	}

	timestamp := time.Now().UnixMilli()
	tickerPrices := make(map[string]types.TickerPrice)

	for _, hostZone := range hostZoneResp.HostZones {
		if symbol, ok := hostDenoms[hostZone.HostDenom]; ok {
			tickerPrices[symbol] = types.TickerPrice{
				Price:  sdk.MustNewDecFromStr(hostZone.RedemptionRate),
				Volume: sdk.NewDec(1),
				Time:   timestamp,
			}
		}
	}

	fmt.Println(tickerPrices)

	return tickerPrices, nil
}

// SubscribeCurrencyPairs performs a no-op since fin does not use websockets
func (p *StrideProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	return nil
}

func (p *StrideProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	return nil
}

func (p *StrideProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	return types.CurrencyPair{}, true
}

func (p *StrideProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	return nil
}

func (p *StrideProvider) SetSubscribedPair(cp types.CurrencyPair) {}

// GetAvailablePairs return all available pairs symbol to susbscribe.
func (p *StrideProvider) GetAvailablePairs() (map[string]struct{}, error) {
	// not used yet, so skipping this unless needed
	return make(map[string]struct{}, 0), nil
}
