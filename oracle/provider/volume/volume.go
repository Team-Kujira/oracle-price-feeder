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
	"golang.org/x/exp/slices"

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
	symbols  []string
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
	pairs []types.CurrencyPair,
	period int64,
) (VolumeHandler, error) {
	symbols := []string{}
	for _, pair := range pairs {
		symbols = append(symbols, pair.String())
		symbols = append(symbols, pair.Swap().String())
	}

	handler := VolumeHandler{
		logger:   logger,
		db:       db,
		provider: provider,
		symbols:  symbols,
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

	symbols := map[string]struct{}{}

	for _, symbol := range h.symbols {
		h.totals[symbol] = sdk.ZeroDec()
		symbols[symbol] = struct{}{}
	}

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

		_, found := symbols[symbol]
		if !found {
			continue
		}

		_, found = volumeMap[height]
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

	// add volumes and calculate totals + missing
	h.update(volumes)

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

	sort.Slice(volumes, func(i, j int) bool {
		return volumes[i].Height < volumes[j].Height
	})

	slices.CompactFunc(volumes, func(e1, e2 Volume) bool {
		return e1.Height == e2.Height
	})

	if volumes[0].Height == 0 {
		return
	}

	if len(h.volumes) == 0 {
		h.volumes = []Volume{volumes[0]}
	}

	knownMinHeight := h.volumes[0].Height
	knownMaxHeight := h.volumes[len(h.volumes)-1].Height
	newMinHeight := volumes[0].Height
	newMaxHeight := volumes[len(volumes)-1].Height

	fmt.Printf(
		"%d - %d\n%d - %d\n",
		knownMinHeight, knownMaxHeight, newMinHeight, newMaxHeight,
	)

	if knownMaxHeight > newMinHeight && knownMinHeight < newMaxHeight {
		// [4, 6, 7] + [5, 8]
		// [4, 6, 7] + [3, 5]
		// [4, 6, 7] + [5]
		h.update(volumes)
	} else {
		if newMinHeight > knownMaxHeight {
			h.append(volumes)
		}

		if newMaxHeight < knownMinHeight {
			h.prepend(volumes)
		}
	}

	if len(h.volumes) < 2 {
		return
	}

	stopTime := h.volumes[len(h.volumes)-1].Time
	startTime := stopTime - h.period

	go func() {
		err := h.persist(volumes)
		if err != nil {
			h.logger.Error().Msg("error writing volumes to database")
			return
		}

		_, err = h.cleanup.Exec(startTime)
		if err != nil {
			h.logger.Err(err).Msg("failed removing old volumes")
		}
	}()

	first := h.volumes[0]
	last := h.volumes[len(h.volumes)-1]

	blockTime := float64(last.Time-first.Time) / float64(len(h.volumes))

	blocks := uint64(math.Round(float64(first.Time-startTime) / blockTime))
	if first.Height-blocks < first.Height-10 {
		height := first.Height - blocks
		diff := first.Height - height
		missing := make([]uint64, diff)
		fmt.Println(">", first.Height-blocks, blocks, diff)
		for i := range missing {
			missing[i] = height + uint64(i)
		}

		h.missing = append(missing, h.missing...)
	}

	fmt.Println("Volume Calculation:", time.Since(t0))
}

func (h *VolumeHandler) append(volumes []Volume) {
	t0 := time.Now()
	h.logger.Info().Msg("append")
	// [4, 6, 7] + [8, 9]

	stopTime := volumes[len(volumes)-1].Time
	startTime := stopTime - h.period

	startIndex := 0
	startHeight := uint64(0)

	// remove old data
	for i, volume := range h.volumes {
		startIndex = i
		startHeight = volume.Height
		if volume.Time >= startTime {
			break
		}

		for symbol, value := range volume.Values {
			total, found := h.totals[symbol]
			if !found {
				continue
			}
			h.totals[symbol] = total.Sub(value)
		}
	}

	// remove outdated missing blocks
	var missing []uint64
	for i, height := range h.missing {
		if height > startHeight {
			missing = h.missing[i:]
			break
		}
	}
	h.missing = missing

	// search for new missing blocks
	height := h.volumes[len(h.volumes)-1].Height + 1
	for height < volumes[0].Height {
		h.missing = append(h.missing, height)
		height++
	}

	// add new data
	for _, volume := range volumes {
		for symbol, value := range volume.Values {
			total, found := h.totals[symbol]
			if !found {
				continue
			}
			h.totals[symbol] = total.Add(value)
		}
	}

	h.volumes = append(h.volumes[startIndex:], volumes...)
	fmt.Println("append:", time.Since(t0))
}

func (h *VolumeHandler) prepend(volumes []Volume) {
	t0 := time.Now()
	h.logger.Info().Msg("prepend")
	// [4, 6, 7] + [1, 2]

	stopTime := h.volumes[len(h.volumes)-1].Time
	startTime := stopTime - h.period

	startIndex := 0

	for i := len(volumes) - 1; i > 0; i-- {
		volume := volumes[i]
		if volume.Time < startTime {
			startIndex = i
			break
		}

		for symbol, value := range volume.Values {
			total, found := h.totals[symbol]
			if !found {
				continue
			}
			h.totals[symbol] = total.Add(value)
		}

		index := slices.Index(h.missing, volume.Height)
		if index < 0 {
			continue
		}
		h.missing = slices.Delete(h.missing, index, index+1)
	}

	h.volumes = append(volumes[startIndex:], h.volumes...)
	fmt.Println("prepend:", time.Since(t0))
}

func (h *VolumeHandler) update(volumes []Volume) {
	t0 := time.Now()

	if len(volumes) == 0 {
		return
	}

	h.logger.Info().Msg("update")

	h.volumes = append(h.volumes, volumes...)

	sort.Slice(h.volumes, func(i, j int) bool {
		return h.volumes[i].Height < h.volumes[j].Height
	})

	slices.CompactFunc(h.volumes, func(e1, e2 Volume) bool {
		return e1.Height == e2.Height
	})

	stopTime := h.volumes[len(h.volumes)-1].Time
	startTime := stopTime - h.period

	startIndex := 0

	for i, volume := range h.volumes {
		if volume.Time > startTime {
			startIndex = i
			break
		}
	}

	h.volumes = h.volumes[startIndex:]

	// reset totals
	for symbol := range h.totals {
		h.totals[symbol] = sdk.ZeroDec()
	}

	// calculate totals and find missing blocks
	blocks := h.volumes[len(h.volumes)-1].Height - h.volumes[0].Height
	missing := make([]uint64, blocks)
	index := 0
	for i, volume := range h.volumes {
		for symbol, value := range volume.Values {
			total, found := h.totals[symbol]
			if !found {
				continue
			}
			h.totals[symbol] = total.Add(value)
		}

		if i == 0 {
			continue
		}

		height := h.volumes[i-1].Height + 1
		for height < volume.Height {
			missing[index] = height
			index++
			height++
		}
	}

	h.missing = missing[:index]

	fmt.Println("update:", time.Since(t0))
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
	missing := len(h.missing)
	volumes := len(h.volumes)
	percent := float64(missing) / float64(volumes) * 100
	fmt.Printf("--- %s @ %s ---\n", symbol, h.provider)
	fmt.Println("Volumes:", volumes)
	fmt.Printf("Missing: %d (%.2f%%)\n", missing, percent)
	total, found := h.totals[symbol]
	if !found {
		return
	}
	fmt.Println("Total:", total)
}
