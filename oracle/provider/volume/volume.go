package volume

import (
	"database/sql"
	"fmt"
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

type TotalVolume struct {
	Period  int64
	Total   sdk.Dec
	Volumes []Volume
	Missing []uint64
}

func (v *TotalVolume) Add(volumes []Volume) {
	sort.Slice(volumes, func(i, j int) bool {
		return volumes[i].Height < volumes[j].Height
	})

	v.Volumes = append(v.Volumes, volumes...)

	// validate and calculate total
	v.Missing = []uint64{}
	valid := []Volume{}
	total := sdk.ZeroDec()
	stop := time.Now().Unix()
	start := stop - v.Period

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

	v.Total = total
	v.Volumes = valid
}

func (v *TotalVolume) Debug() {
	fmt.Println("Total:", v.Total)
	fmt.Println("Missing:", v.Missing)
	fmt.Println("Volumes:", v.Volumes)
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
	totals   map[string]TotalVolume
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
		period:   300,
	}

	return history, history.Init()
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

	h.totals = map[string]TotalVolume{}
	for _, symbol := range h.symbols {
		total := TotalVolume{
			Period: h.period,
		}

		values, found := volumes[symbol]
		if found {
			total.Add(values)
		}

		h.totals[symbol] = total
	}

	return nil
}

func (h *VolumeHistory) AddVolumes(
	height uint64,
	timestamp time.Time,
	volumes map[string]sdk.Dec,
) error {
	if len(volumes) == 0 {
		return nil
	}

	tmpl := `
		INSERT OR IGNORE INTO volume_history(
			symbol, provider, block, time, volume
		) VALUES %s
	`
	placeholders := make([]string, len(volumes))

	for i := range placeholders {
		placeholders[i] = "(?, ?, ?, ?, ?)"
	}

	values := []interface{}{}

	for symbol, volume := range volumes {
		values = append(values, []interface{}{
			symbol, h.provider, height, timestamp.Unix(), volume.String(),
		}...)
	}

	sql := fmt.Sprintf(tmpl, strings.Join(placeholders, ", "))

	_, err := h.db.Exec(sql, values...)
	if err != nil {
		return h.error(err)
	}

	for symbol, volume := range volumes {
		total, found := h.totals[symbol]
		if !found {
			total = TotalVolume{
				Period: h.period,
				Total:  sdk.ZeroDec(),
			}
		}

		h.logger.Info().Str("symbol", symbol).Msg("add volume")

		total.Add([]Volume{{
			Height: height,
			Time:   timestamp.Unix(),
			Value:  volume,
		}})

		h.totals[symbol] = total
	}

	return nil
}

func (h *VolumeHistory) Debug() {
	for symbol, total := range h.totals {
		if total.Total.IsNil() || total.Total.IsZero() {
			continue
		}
		fmt.Println("> ", symbol)
		total.Debug()
		return
	}
}
