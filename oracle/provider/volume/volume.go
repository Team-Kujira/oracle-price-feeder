package volume

import (
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	_ "github.com/mattn/go-sqlite3"

	"github.com/rs/zerolog"
)

type Volume struct {
	Height uint64
	Time   int64
	Value  sdk.Dec
}

type Volumes struct {
	Height uint64
	Time   int64
	Values map[string]sdk.Dec
}

type TotalVolume struct {
	Period  int64
	Total   sdk.Dec
	Volumes []Volume
	Missing []uint64
}

func (v *TotalVolume) Calculate() {
	if len(v.Volumes) < 2 {
		return
	}

	sort.Slice(v.Volumes, func(i, j int) bool {
		return v.Volumes[i].Height < v.Volumes[j].Height
	})

	// use the most recent block known
	stop := v.Volumes[len(v.Volumes)-1].Time
	start := stop - v.Period

	// validate and calculate total
	v.Missing = []uint64{}
	valid := []Volume{}
	total := sdk.ZeroDec()

	for i, volume := range v.Volumes {
		if volume.Time < start {
			continue
		}

		if volume.Time > stop {
			continue
		}

		if i != 0 {
			// skip duplicate
			if v.Volumes[i-1].Height == volume.Height {
				continue
			}

			// find missing
			height := v.Volumes[i-1].Height + 1
			for height < volume.Height {
				v.Missing = append(v.Missing, height)
				height++
			}
		}

		valid = append(valid, volume)
		total = total.Add(volume.Value)
	}

	first := valid[0]
	last := valid[len(valid)-1]

	blockTime := float64(last.Time-first.Time) / float64(len(valid))

	expected := first.Height - uint64(math.Round(float64(first.Time-start)/blockTime))
	fmt.Println("EXPECTED:", expected, "-", first.Height)

	if expected+10 < first.Height {
		missing := make([]uint64, first.Height-expected)
		for i := range missing {
			missing[i] = expected + uint64(i)
		}

		v.Missing = append(missing, v.Missing...)
	}

	v.Total = total
	v.Volumes = valid
}

func (v *TotalVolume) Debug() {
	var missing []uint64
	amount := len(v.Missing)
	if amount > 3 {
		missing = v.Missing[amount-3:]
	} else {
		missing = v.Missing
	}

	fmt.Println("Total:", v.Total)

	fmt.Println("Missing:", missing)
	// fmt.Println("Volumes:", v.Volumes)
	fmt.Printf("Ratio: %d/%d\n", len(v.Missing), len(v.Volumes))
}

type VolumeHistory struct {
	logger   zerolog.Logger
	db       *sql.DB
	insert   *sql.Stmt
	query    *sql.Stmt
	cleanup  *sql.Stmt
	provider string
	period   int64 // period to sum the volumes over (24h)
	totals   map[string]*TotalVolume
	symbols  []string
}

func NewVolumeHistory(
	logger zerolog.Logger,
	db *sql.DB,
	provider string,
	pairs []types.CurrencyPair,
) (VolumeHistory, error) {
	logger = logger.With().Str("module", "volume").Logger()

	symbols := make([]string, len(pairs)*2)
	for i, pair := range pairs {
		symbols[2*i+0] = pair.Base + pair.Quote
		symbols[2*i+1] = pair.Quote + pair.Base
	}

	history := VolumeHistory{
		logger:   logger,
		db:       db,
		provider: provider,
		symbols:  symbols,
		period:   60 * 60 * 24,
	}

	return history, history.Init()
}

func (h *VolumeHistory) GetVolume(symbol string) sdk.Dec {
	total, found := h.totals[symbol]
	if !found {
		return sdk.ZeroDec()
	}

	if len(total.Volumes) < 10 {
		return sdk.ZeroDec()
	}

	// < 10%
	if len(total.Missing)*10 > len(total.Volumes) {
		return sdk.ZeroDec()
	}

	return total.Total
}

func (h *VolumeHistory) error(err error) error {
	h.logger.Err(err).Msg("")
	return err
}

func (h *VolumeHistory) Init() error {
	_, err := h.db.Exec(`
		CREATE TABLE IF NOT EXISTS volume_history(
			block INT NOT NULL,
			symbol TEXT NOT NULL,
			provider TEXT NOT NULL,
			time INT NOT NULL,
			volume TEXT NOT NULL,
			CONSTRAINT id PRIMARY KEY (symbol, provider, block, time)
		)`)
	if err != nil {
		return h.error(err)
	}

	_, err = h.db.Exec("VACUUM")
	if err != nil {
		return h.error(err)
	}

	insert, err := h.db.Prepare(`
		INSERT INTO volume_history(symbol, provider, block, time, volume)
        SELECT ?, ?, ?, ?, ?
        WHERE NOT EXISTS (SELECT 1 FROM volume_history WHERE
			symbol = ? AND provider = ? AND block = ?
		)
    `)
	if err != nil {
		return h.error(err)
	}

	query, err := h.db.Prepare(`
		SELECT provider, time, volume FROM volume_history
        WHERE symbol = ? AND provider = ? AND time BETWEEN ? AND ?
        ORDER BY time ASC
    `)
	if err != nil {
		return h.error(err)
	}

	cleanup, err := h.db.Prepare(`
		DELETE from volume_history
		WHERE time < ?
	`)
	if err != nil {
		return h.error(err)
	}

	h.insert = insert
	h.query = query
	h.cleanup = cleanup

	return h.InitStatusFromDb()
}

func (h *VolumeHistory) InitStatusFromDb() error {
	stop := time.Now().Unix()
	start := stop - h.period

	rows, err := h.db.Query(`
		SELECT symbol, block, time, volume FROM volume_history
		WHERE provider = ? AND time BETWEEN ? AND ?
		ORDER BY symbol, block
	`, h.provider, start, stop)
	if err != nil {
		h.logger.Err(err).Msg("failed initializing status from db")
		return err
	}
	defer rows.Close()

	volumes := map[string][]Volume{}

	for rows.Next() {
		var (
			symbol, volume string
			height         uint64
			timestamp      int64
		)

		err = rows.Scan(&symbol, &height, &timestamp, &volume)
		if err != nil {
			h.logger.Err(err).Msg("")
			return err
		}

		_, found := volumes[symbol]
		if !found {
			volumes[symbol] = []Volume{}
		}

		volumeDec, err := sdk.NewDecFromStr(volume)
		if err != nil {
			h.logger.Err(err).Msg("")
			return err
		}

		volumes[symbol] = append(volumes[symbol], Volume{
			Height: height,
			Time:   timestamp,
			Value:  volumeDec,
		})
	}

	h.totals = map[string]*TotalVolume{}
	for _, symbol := range h.symbols {
		total := TotalVolume{
			Period: h.period,
		}

		values, found := volumes[symbol]
		if found {
			total.Volumes = append(total.Volumes, values...)
		}

		h.totals[symbol] = &total
	}

	return nil
}

func (h *VolumeHistory) AddVolumes(
	volumes []Volumes,
) error {
	t0 := time.Now()
	fmt.Println("ADD VOLUMES")

	tmpl := `
		INSERT OR IGNORE INTO volume_history(
			symbol, provider, block, time, volume
		) VALUES %s
	`

	placeholders := []string{}
	values := []interface{}{}

	for _, volume := range volumes {
		if len(volume.Values) == 0 {
			h.logger.Warn().Msg("no volumes provided")
			return nil
		}

		for symbol, value := range volume.Values {
			placeholders = append(placeholders, "(?, ?, ?, ?, ?)")

			values = append(values, []interface{}{
				symbol,
				h.provider,
				volume.Height,
				volume.Time,
				value.String(),
			}...)

			total, found := h.totals[symbol]
			if !found {
				total = &TotalVolume{
					Period: h.period,
					Total:  sdk.ZeroDec(),
				}
			}

			total.Volumes = append(total.Volumes, Volume{
				Height: volume.Height,
				Time:   volume.Time,
				Value:  value,
			})

			// h.totals[symbol] = total
		}
	}

	sql := fmt.Sprintf(tmpl, strings.Join(placeholders, ", "))

	_, err := h.db.Exec(sql, values...)
	if err != nil {
		return h.error(err)
	}

	t1 := time.Now()

	for symbol, total := range h.totals {
		total.Calculate()
		h.totals[symbol] = total
	}

	t2 := time.Now()

	fmt.Println("add volumes:", t1.Sub(t0))
	fmt.Println("recalculate:", t2.Sub(t1))
	fmt.Println("total:      ", t2.Sub(t0))

	return nil
}

func (h *VolumeHistory) GetLatestMissing(amount int) []uint64 {
	for _, total := range h.totals {
		if len(total.Missing) > amount {
			return total.Missing[len(total.Missing)-amount:]
		}
		return total.Missing
	}
	return []uint64{}
}

func (h *VolumeHistory) Debug(symbol string) {
	total, found := h.totals[symbol]
	if !found {
		return
	}
	fmt.Println("-- History -- ")
	fmt.Println(symbol)
	total.Debug()
}
