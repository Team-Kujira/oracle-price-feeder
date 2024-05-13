package volume

import (
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
	_ "github.com/mattn/go-sqlite3"

	"github.com/rs/zerolog"
)

type Volume struct {
	Height uint64
	Time   int64
	Values map[string]sdk.Dec
}

type VolumeHandler struct {
	logger   zerolog.Logger
	db       *sql.DB
	provider string
	totals   map[string]sdk.Dec
	volumes  []Volume
	period   int64
	missing  []uint64
	cleanup  *sql.Stmt
}

func NewVolumeHandler(
	logger zerolog.Logger,
	db *sql.DB,
	provider string,
	period int64,
) (VolumeHandler, error) {
	handler := VolumeHandler{
		logger:   logger,
		db:       db,
		provider: provider,
		totals:   map[string]sdk.Dec{},
		volumes:  []Volume{},
		period:   period,
		missing:  []uint64{},
	}

	err := handler.init()
	if err != nil {
		return handler, err
	}

	return handler, nil
}

func (h *VolumeHandler) init() error {
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
		h.logger.Err(err).Msg("failed creating table")
		return err
	}

	_, err = h.db.Exec("VACUUM")
	if err != nil {
		h.logger.Err(err).Msg("")
		return err
	}

	h.cleanup, err = h.db.Prepare(`
		DELETE from volume_history
		WHERE time < ?
	`)
	if err != nil {
		h.logger.Err(err).Msg("failed creating cleanup statement")
		return err
	}

	return h.load()
}

func (h *VolumeHandler) load() error {
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

	volumeMap := map[uint64]Volume{}

	for rows.Next() {
		var (
			symbol, value string
			height        uint64
			timestamp     int64
		)

		err = rows.Scan(&symbol, &height, &timestamp, &value)
		if err != nil {
			h.logger.Err(err).Msg("")
			return err
		}

		_, found := volumeMap[height]
		if !found {
			volumeMap[height] = Volume{
				Height: height,
				Time:   timestamp,
				Values: map[string]sdk.Dec{},
			}
		}

		volume, err := sdk.NewDecFromStr(value)
		if err != nil {
			h.logger.Err(err).Msg("")
			return err
		}

		volumeMap[height].Values[symbol] = volume
	}

	volumes := []Volume{}
	for _, volume := range volumeMap {
		volumes = append(volumes, volume)
	}

	// don't use Add(), we don't need to persist the data
	h.volumes = volumes

	return nil
}

func (h *VolumeHandler) Get(symbol string) sdk.Dec {
	total, found := h.totals[symbol]
	if !found {
		return sdk.ZeroDec()
	}

	if len(h.volumes) < 10 {
		return sdk.ZeroDec()
	}

	// < 10%
	if len(h.missing)*10 > len(h.volumes) {
		return sdk.ZeroDec()
	}

	return total
}

func (h *VolumeHandler) Add(volumes []Volume) {
	t0 := time.Now()
	if len(volumes) == 0 {
		return
	}

	h.volumes = append(h.volumes, volumes...)

	if len(h.volumes) < 2 {
		return
	}

	sort.Slice(h.volumes, func(i, j int) bool {
		return h.volumes[i].Height < h.volumes[j].Height
	})

	// use the most recent block known
	stopTime := h.volumes[len(h.volumes)-1].Time
	startTime := stopTime - h.period

	startIndex := 0
	stopIndex := len(h.volumes)

	missing := []uint64{}
	totals := map[string]sdk.Dec{}

	for i, volume := range h.volumes {
		if volume.Time < startTime {
			startIndex = i + 1
			continue
		}

		if volume.Time > stopTime {
			stopIndex = i
			break
		}

		if i > 0 {
			// skip duplicate
			if h.volumes[i-1].Height == volume.Height {
				continue
			}

			// find missing blocks
			height := h.volumes[i-1].Height + 1
			for height < volume.Height {
				missing = append(missing, height)
				height++
			}

		}

		for symbol, value := range volume.Values {
			total, found := totals[symbol]
			if !found {
				total = sdk.ZeroDec()
			}
			totals[symbol] = total.Add(value)
		}
	}

	h.volumes = h.volumes[startIndex:stopIndex]
	h.missing = missing
	h.totals = totals

	err := h.persist(volumes)
	if err != nil {
		h.logger.Error().Msg("error writing volumes to database")
		return
	}

	_, err = h.cleanup.Exec(startTime)
	if err != nil {
		h.logger.Err(err).Msg("failed removing old volumes")
	}

	if len(h.volumes) < 2 {
		return
	}

	first := h.volumes[0]
	last := h.volumes[len(h.volumes)-1]

	blockTime := float64(last.Time-first.Time) / float64(len(h.volumes))

	blocks := uint64(math.Round(float64(first.Time-startTime) / blockTime))
	if first.Height-blocks < first.Height-10 {
		missing := []uint64{}
		for height := first.Height - blocks; height < first.Height; height++ {
			missing = append(missing, height)
		}

		h.missing = append(missing, h.missing...)
	}

	fmt.Println("Volume Calculation:", time.Now().Sub(t0))
}

func (h *VolumeHandler) persist(volumes []Volume) error {
	if len(volumes) == 0 {
		return nil
	}

	placeholders := []string{}
	values := []interface{}{}

	for _, volume := range volumes {
		if len(volume.Values) == 0 {
			h.logger.Warn().
				Uint64("height", volume.Height).
				Msg("no values found")
			continue
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
		}
	}

	if len(placeholders) == 0 {
		return nil
	}

	sql := fmt.Sprintf(`
		INSERT OR IGNORE INTO volume_history(
			symbol, provider, block, time, volume
		) VALUES %s
	`, strings.Join(placeholders, ", "))

	_, err := h.db.Exec(sql, values...)
	if err != nil {
		h.logger.Err(err).Msg("failed inserting volumes")
		return err
	}

	return nil
}

func (h *VolumeHandler) GetMissing(amount int) []uint64 {
	if len(h.missing) > amount {
		return h.missing[len(h.missing)-amount:]
	}
	return h.missing
}

func (h *VolumeHandler) Debug(symbol string) {
	fmt.Println("Volumes:", len(h.volumes))
	fmt.Println("Missing:", len(h.missing))
	total, found := h.totals[symbol]
	if !found {
		return
	}
	fmt.Println("Total:", total)
}
