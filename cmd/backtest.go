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

			interval, err := cmd.Flags().GetInt64("interval")
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

			var tickers []types.TickerPrice
			json.Unmarshal(bz, &tickers)

			var first, last time.Time

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

			first = first.Round(time.Second * time.Duration(interval))
			last = last.Round(time.Second * time.Duration(interval))

			tick := first

			for tick.Before(last) {
				tick = tick.Add(time.Second * time.Duration(interval))
				start := tick.Add(time.Second * -1 * time.Duration(period))
				tvwap, _, err := derivative.Twap(tickers, start, tick)
				if err != nil {
					fmt.Printf("%s;\n", tick.UTC())
				} else {
					fmt.Printf("%s;%s\n", tick.UTC(), tvwap)
				}
			}

			return nil
		},
	}

	backtestCmd.PersistentFlags().Int64("period", 1800, "Time period of the TVWAP calculation in seconds")
	backtestCmd.PersistentFlags().Int64("interval", 60, "Interval in which new TVWAP prices are calculated in seconds")

	return backtestCmd
}
