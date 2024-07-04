package types

import sdk "github.com/cosmos/cosmos-sdk/types"

type Denom struct {
	Amount sdk.Dec
	Symbol string
}
