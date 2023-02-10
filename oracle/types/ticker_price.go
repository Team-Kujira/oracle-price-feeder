package types

import (
	"fmt"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
)

// TickerPrice defines price and volume information for a symbol or ticker exchange rate.
type TickerPrice struct {
	Price  sdk.Dec // last trade price
	Volume sdk.Dec // 24h volume
	Time   time.Time
}

func NewTickerPrice(price string, volume string, timestamp time.Time) (TickerPrice, error) {
	priceDec, err := sdk.NewDecFromStr(price)
	if err != nil {
		return TickerPrice{}, fmt.Errorf("failed to convert ticker price: %v", err)
	}
	volumeDec, err := sdk.NewDecFromStr(volume)
	if err != nil {
		return TickerPrice{}, fmt.Errorf("failed to convert ticker volume: %v", err)
	}
	ticker := TickerPrice{
		Price: priceDec,
		Volume: volumeDec,
		Time: timestamp,
	}
	return ticker, nil
}
