package types

import (
	"strings"
)

// CurrencyPair defines a currency exchange pair consisting of a base and a quote.
// We primarily utilize the base for broadcasting exchange rates and use the
// pair for querying for the ticker prices.
type CurrencyPair struct {
	Base  string
	Quote string
}

// String implements the Stringer interface and defines a ticker symbol for
// querying the exchange rate.
func (cp CurrencyPair) String() string {
	return strings.ToUpper(cp.Base + cp.Quote)
}

// Join returns the base- and quote denoms seperated by provided string
func (cp CurrencyPair) Join(seperator string) string {
	return strings.ToUpper(cp.Base + seperator + cp.Quote)
}

// Swap returns a CurrencyPair with quote and denom swapped
func (cp CurrencyPair) Swap() CurrencyPair {
	return CurrencyPair{Base: cp.Quote, Quote: cp.Base}
}

// MapPairsToSlice returns the map of currency pairs as slice.
func MapPairsToSlice(mapPairs map[string]CurrencyPair) []CurrencyPair {
	currencyPairs := make([]CurrencyPair, len(mapPairs))

	iterator := 0
	for _, cp := range mapPairs {
		currencyPairs[iterator] = cp
		iterator++
	}

	return currencyPairs
}
