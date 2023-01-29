package types

import (
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
)

// TickerPrice defines price and volume information for a symbol or ticker exchange rate.
type TickerPrice struct {
	Price  sdk.Dec // last trade price
	Volume sdk.Dec // 24h volume
	Time   time.Time
}
