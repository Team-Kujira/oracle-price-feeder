package volume

import (
	sdk "github.com/cosmos/cosmos-sdk/types"
)

type Total struct {
	Total   sdk.Dec
	Values  int
	Missing int
}

func NewTotal() *Total {
	return &Total{
		Total:  sdk.ZeroDec(),
		Values: 0,
	}
}

func (t *Total) Clear() {
	t.Total = sdk.ZeroDec()
	t.Values = 0
}

func (t *Total) Sub(value sdk.Dec) {
	if value.IsNil() || value.IsNegative() {
		return
	}
	t.Total = t.Total.Sub(value)
	t.Values -= 1
}

func (t *Total) Add(value sdk.Dec) {
	if value.IsNil() || value.IsNegative() {
		return
	}
	t.Total = t.Total.Add(value)
	t.Values += 1
}
