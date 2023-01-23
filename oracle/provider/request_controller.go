package provider

import (
	"context"
	"time"

	"github.com/rs/zerolog"
)

type (
	RequestHandler           func() error
	GenericRequestController struct {
		parentCtx       context.Context
		requestInterval time.Duration
		requestHandler  RequestHandler
		logger          zerolog.Logger
	}
)

func NewGenericRequestController(
	ctx context.Context,
	requestInterval time.Duration,
	requestHandler RequestHandler,
	logger zerolog.Logger,
) *GenericRequestController {
	return &GenericRequestController{
		parentCtx:       ctx,
		requestInterval: requestInterval,
		requestHandler:  requestHandler,
		logger:          logger,
	}
}

func (grc *GenericRequestController) Start() {
	go grc.requestLoop()
}

func (grc *GenericRequestController) requestLoop() {
	requestTicker := time.NewTicker(grc.requestInterval)

	for {
		grc.logger.Debug().Msg("process request handler")
		err := grc.requestHandler()
		if err != nil {
			continue
		}
		select {
		case <-grc.parentCtx.Done():
			return
		case <-requestTicker.C:
			continue
		}
	}
}
