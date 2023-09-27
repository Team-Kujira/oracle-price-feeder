package client

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	rpcclient "github.com/tendermint/tendermint/rpc/client"
)

type ChainHeight struct {
	Logger       zerolog.Logger
	ctx          context.Context
	rpc          rpcclient.Client
	pollInterval time.Duration
	height       int64
	err          error
}

func NewChainHeight(
	ctx context.Context,
	rpc rpcclient.Client,
	logger zerolog.Logger,
	pollInterval time.Duration,
) (*ChainHeight, error) {
	if !rpc.IsRunning() {
		err := rpc.Start()
		if err != nil {
			return nil, err
		}
	}
	c := &ChainHeight{
		Logger:       logger.With().Str("oracle_client", "chain_height").Logger(),
		ctx:          ctx,
		rpc:          rpc,
		height:       0,
		pollInterval: pollInterval,
		err:          nil,
	}
	c.update()
	go c.poll()
	return c, c.err
}

func (c *ChainHeight) poll() {
	for {
		time.Sleep(c.pollInterval)
		c.update()
	}
}

func (c *ChainHeight) update() {
	status, err := c.rpc.Status(c.ctx)
	if err == nil {
		if c.height < status.SyncInfo.LatestBlockHeight {
			c.height = status.SyncInfo.LatestBlockHeight
			c.Logger.Info().Int64("height", c.height).Msg("got new chain height")
		} else {
			c.Logger.Debug().
				Int64("new", status.SyncInfo.LatestBlockHeight).
				Int64("current", c.height).
				Msg("ignoring stale chain height")
		}
	} else {
		c.Logger.Warn().Err(err).Msg("failed to get chain height")
	}
	c.err = err
}

func (c *ChainHeight) GetChainHeight() (int64, error) {
	return c.height, c.err
}
