package v1

import (
	sdk "github.com/cosmos/cosmos-sdk/types"
)

// Oracle defines the Oracle interface contract that the v1 router depends on.
type Oracle interface {
	GetPrices() sdk.DecCoins
}
