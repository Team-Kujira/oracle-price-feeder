package history

import (
	"database/sql"
    "time"

    "price-feeder/oracle/types"
    "price-feeder/oracle/provider"

    _ "github.com/mattn/go-sqlite3"
    "github.com/rs/zerolog"
)

type (
    PriceHistory struct {
        db *sql.DB
        insert *sql.Stmt
        query *sql.Stmt
        logger zerolog.Logger
    }
)

func NewPriceHistory(path string, logger zerolog.Logger) (PriceHistory, error) {
    db, err := sql.Open("sqlite3", path)
    if err != nil {
        logger.Error().Err(err).Str("path", path).Msg("failed to open sqlite db")
        return PriceHistory{}, err
    }
    p := PriceHistory{
        db: db,
        logger: logger.With().Str("module", "history").Logger(),
    }
    return p, p.Init()
}

func (p *PriceHistory) Init() error {
    _, err := p.db.Exec(`CREATE TABLE IF NOT EXISTS crypto_ticker_prices(
        symbol TEXT NOT NULL,
        provider TEXT NOT NULL,
        time INT NOT NULL,
        price TEXT NOT NULL,
        volume TEXT NOT NULL,
        CONSTRAINT id PRIMARY KEY (symbol, provider, time)
    )`)
    if err != nil {
        p.logger.Error().Err(err).Msg("failed to create db table")
        return err
    }
    insert, err := p.db.Prepare(`INSERT INTO crypto_ticker_prices(symbol, provider, time, price, volume)
        SELECT ?, ?, ?, ?, ?
        WHERE NOT EXISTS (SELECT 1 FROM crypto_ticker_prices WHERE symbol = ? AND provider = ? AND time = ?)
    `)
    if err != nil {
        p.logger.Error().Err(err).Msg("failed to prepare sql insert statement")
        return err
    }
    query, err := p.db.Prepare(`SELECT provider, time, price, volume FROM crypto_ticker_prices
        WHERE symbol = ? AND time BETWEEN ? AND ?
        ORDER BY time DESC
    `)
    if err != nil {
        p.logger.Error().Err(err).Msg("failed to prepare sql query statement")
        return err
    }
    p.insert = insert
    p.query = query
    return nil
}

func (p *PriceHistory) AddTickerPrice(pair types.CurrencyPair, provider provider.Name, ticker types.TickerPrice) error {
    _, err := p.insert.Exec(
        pair.String(),
        provider.String(),
        ticker.Time.Unix(),
        ticker.Price.String(),
        ticker.Volume.String(),
        pair.String(),
        provider.String(),
        ticker.Time.Unix(),
    )
    if err != nil {
        p.logger.Error().Err(err).Str("pair", pair.String()).Str("provider", provider.String()).Msg("failed to store ticker")
    }
    return err
}

func (p *PriceHistory) GetTickerPrices(
    pair types.CurrencyPair,
    start time.Time,
    end time.Time,
) (map[string][]types.TickerPrice, error) {
    rows, err := p.query.Query(pair.String(), start.Unix(), end.Unix())
    if err != nil {
        p.logger.Error().Err(err).Str("pair", pair.String()).Msg("failed to query stored ticker prices")
        return nil, err
    }
    defer rows.Close()
    tickers := map[string][]types.TickerPrice{}
    for rows.Next() {
        var epochTime int64
        var providerName, price, volume string
        err := rows.Scan(&providerName, &epochTime, &price, &volume)
        if err != nil {
            p.logger.Error().Err(err).Str("pair", pair.String()).Msg("failed to parse ticker query results")
            return nil, err
        }
        ticker, err := types.NewTickerPrice(price, volume, time.Unix(epochTime, 0))
        if err != nil {
            p.logger.Error().Err(err).Str("pair", pair.String()).Msg("failed to create ticker")
        }
        providerTickers, ok := tickers[providerName]
        if !ok {
            tickers[providerName] = []types.TickerPrice{ticker}
        } else {
            tickers[providerName] = append(providerTickers, ticker)
        }
    }
    err = rows.Err()
    if err != nil {
        p.logger.Error().Err(err).Str("pair", pair.String()).Msg("failed to read all stored tickers")
        return nil, err
    }
    return tickers, nil
}
