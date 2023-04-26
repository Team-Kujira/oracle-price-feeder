package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"price-feeder/oracle/derivative"
	"price-feeder/oracle/types"
	"time"

	"github.com/spf13/cobra"
)

func getBacktestCmd() *cobra.Command {
	backtestCmd := &cobra.Command{
		Use:   "backtest",
		Short: "Backtest TWAP for values in provided JSON file",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("no file provided")
			}

			period, err := cmd.Flags().GetInt64("period")
			if err != nil {
				return err
			}

			file, err := os.Open(args[0])
			if err != nil {
				return err
			}

			defer file.Close()

			bz, err := io.ReadAll(file)
			if err != nil {
				return err
			}

			var tickerMap map[string][]types.TickerPrice
			json.Unmarshal(bz, &tickerMap)

			var first, last time.Time

			for _, tickers := range tickerMap {
				for _, ticker := range tickers {
					if first.IsZero() {
						first = ticker.Time
						last = ticker.Time
						continue
					}

					if ticker.Time.Before(first) {
						first = ticker.Time
					}

					if ticker.Time.After(last) {
						last = ticker.Time
					}
				}
			}

			first = time.Unix(first.Unix()/period*period, 0)
			last = time.Unix(last.Unix()/period*period+period, 0)

			start := first

			for start.Before(last) {
				end := start.Add(time.Second * time.Duration(period))
				tvwap, err := derivative.Tvwap(tickerMap, start, end)
				if err != nil {
					fmt.Println(start.UTC(), err)
				} else {
					fmt.Println(start.UTC(), tvwap)
				}
				start = end
			}

			return nil
		},
	}

	backtestCmd.PersistentFlags().Int64("period", 1800, "Time period for the TVWAP in seconds")

	return backtestCmd
}
