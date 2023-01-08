package provider

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"price-feeder/oracle/types"
)

const (
	finRestURL               = "https://api.kujira.app"
	finPairsEndpoint         = "/api/coingecko/pairs"
	finTickersEndpoint       = "/api/coingecko/tickers"
	finCandlesEndpoint       = "/api/trades/candles"
	finCandleBinSizeMinutes  = 5
	finCandleWindowSizeHours = 240
)

var _ Provider = (*FinProvider)(nil)

type (
	FinProvider struct {
		baseURL string
		client  *http.Client
	}

	FinTickers struct {
		Tickers []FinTicker `json:"tickers"`
	}

	FinTicker struct {
		Base   string `json:"base_currency"`
		Target string `json:"target_currency"`
		Symbol string `json:"ticker_id"`
		Price  string `json:"last_price"`
		Volume string `json:"base_volume"`
	}

	FinPairs struct {
		Pairs []FinPair `json:"pairs"`
	}

	FinPair struct {
		Base    string `json:"base"`
		Target  string `json:"target"`
		Symbol  string `json:"ticker_id"`
		Address string `json:"pool_id"`
	}
)

func NewFinProvider(endpoint Endpoint) *FinProvider {
	if endpoint.Name == ProviderFin {
		return &FinProvider{
			baseURL: endpoint.Rest,
			client:  newDefaultHTTPClient(),
		}
	}
	return &FinProvider{
		baseURL: finRestURL,
		client:  newDefaultHTTPClient(),
	}
}

func (p FinProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	return nil
}

func (p FinProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	return types.CurrencyPair{}, true
}

func (p FinProvider) SetSubscribedPair(cp types.CurrencyPair) {}

// SubscribeCurrencyPairs performs a no-op since fin does not use websockets
func (p FinProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	return nil
}

func (p FinProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	return nil
}

func (p FinProvider) GetTickerPrices(pairs ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	path := fmt.Sprintf("%s%s", p.baseURL, finTickersEndpoint)
	tickerResponse, err := p.client.Get(path)
	if err != nil {
		return nil, fmt.Errorf("FIN tickers request failed: %w", err)
	}
	defer tickerResponse.Body.Close()
	tickerContent, err := ioutil.ReadAll(tickerResponse.Body)
	if err != nil {
		return nil, fmt.Errorf("FIN tickers response read failed: %w", err)
	}
	var tickers FinTickers
	err = json.Unmarshal(tickerContent, &tickers)
	if err != nil {
		return nil, fmt.Errorf("FIN tickers response unmarshal failed: %w", err)
	}
	tickerSymbolPairs := make(map[string]types.CurrencyPair, len(pairs))
	for _, pair := range pairs {
		tickerSymbolPairs[pair.Base+"_"+pair.Quote] = pair
	}
	tickerPrices := make(map[string]types.TickerPrice, len(pairs))

	timestamp := time.Now().UnixMilli()
	for _, ticker := range tickers.Tickers {
		pair, ok := tickerSymbolPairs[strings.ToUpper(ticker.Symbol)]
		if !ok {
			// skip tokens that are not requested
			continue
		}
		_, ok = tickerPrices[pair.String()]
		if ok {
			return nil, fmt.Errorf("FIN tickers response contained duplicate: %s", ticker.Symbol)
		}
		tickerPrices[pair.String()] = types.TickerPrice{
			Price:  strToDec(ticker.Price),
			Volume: strToDec(ticker.Volume),
			Time:   timestamp,
		}
	}
	for _, pair := range pairs {
		_, ok := tickerPrices[pair.String()]
		if !ok {
			return nil, fmt.Errorf("FIN ticker price missing for pair: %s", pair.String())
		}
	}
	return tickerPrices, nil
}

func (p FinProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	return types.TickerPrice{}, nil
}

func (p FinProvider) GetAvailablePairs() (map[string]struct{}, error) {
	finPairs, err := p.getFinPairs()
	if err != nil {
		return nil, err
	}
	availablePairs := make(map[string]struct{}, len(finPairs.Pairs))
	for _, pair := range finPairs.Pairs {
		pair := types.CurrencyPair{
			Base:  strings.ToUpper(pair.Base),
			Quote: strings.ToUpper(pair.Target),
		}
		availablePairs[pair.String()] = struct{}{}
	}
	return availablePairs, nil
}

func (p FinProvider) getFinPairs() (FinPairs, error) {
	path := fmt.Sprintf("%s%s", p.baseURL, finPairsEndpoint)
	pairsResponse, err := p.client.Get(path)
	if err != nil {
		return FinPairs{}, err
	}
	defer pairsResponse.Body.Close()
	var pairs FinPairs
	err = json.NewDecoder(pairsResponse.Body).Decode(&pairs)
	if err != nil {
		return FinPairs{}, err
	}
	return pairs, nil
}
