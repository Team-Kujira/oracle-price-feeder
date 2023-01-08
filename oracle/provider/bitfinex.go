package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"price-feeder/oracle/types"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"github.com/tendermint/tendermint/libs/sync"
)

const (
	bitfinexWSHost   = "api-pub.bitfinex.com"
	bitfinexWSPath   = "/ws/2"
	bitfinexRestHost = "api-pub.bitfinex.com"
)

var _ Provider = (*BitfinexProvider)(nil)

type (
	BitfinexProvider struct {
		wsc             *WebsocketController
		logger          zerolog.Logger
		mtx             sync.Mutex
		endpoints       Endpoint
		tickers         map[uint64]BitfinexTicker
		channels        map[string]uint64
		subscribedPairs map[string]types.CurrencyPair
	}

	BitfinexSubscriptionMsg struct {
		Event   string `json:"event"`
		Channel string `json:"channel"`
		Symbol  string `json:"symbol"`
	}

	BitfinexSubscriptionResponse struct {
		Event     string `json:"event"`
		Channel   string `json:"channel"`
		ChannelID uint64 `json:"chanId"`
		Symbol    string `json:"symbol"`
		Pair      string `json:"pair"`
	}

	// used for storing the last ticker until it is read by getTickerPrice()
	// this is to avoid converting float64 to decimal on every new ticker
	BitfinexTicker struct {
		Price  float64
		Volume float64
		Time   int64
	}
)

func NewBitfinexProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*BitfinexProvider, error) {
	if endpoints.Name != ProviderBitfinex {
		endpoints = Endpoint{
			Name:      ProviderBitfinex,
			Rest:      bitfinexRestHost,
			Websocket: bitfinexWSHost,
		}
	}

	wsURL := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   bitfinexWSPath,
	}

	bitfinexLogger := logger.With().Str("provider", string(ProviderBitfinex)).Logger()

	provider := &BitfinexProvider{
		logger:          bitfinexLogger,
		endpoints:       endpoints,
		tickers:         map[uint64]BitfinexTicker{},
		channels:        map[string]uint64{}, // e.x. channels[ETHUSD] = 137332
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	setSubscribedPairs(provider, pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderBitfinex,
		wsURL,
		provider.GetSubscriptionMsgs(pairs...),
		provider.messageReceived,
		defaultPingDuration,
		websocket.TextMessage,
		bitfinexLogger,
	)

	go provider.wsc.Start()

	return provider, nil
}

func (p *BitfinexProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, len(cps))

	for i, cp := range cps {
		subscriptionMsgs[i] = BitfinexSubscriptionMsg{
			Event:   "subscribe",
			Channel: "ticker",
			Symbol:  cp.String(),
		}
	}

	return subscriptionMsgs
}

func (p *BitfinexProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	cp, ok := p.subscribedPairs[s]
	return cp, ok
}

func (p *BitfinexProvider) SetSubscribedPair(cp types.CurrencyPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.subscribedPairs[cp.String()] = cp
}

func (p *BitfinexProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	return subscribeCurrencyPairs(p, cps)
}

func (p *BitfinexProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return p.wsc.AddSubscriptionMsgs(msgs)
}

func (p *BitfinexProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	return getTickerPrices(p, cps)
}

func (p *BitfinexProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	channel, ok := p.channels[cp.String()]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf("bitfinex failed to get channel id for %s", cp.String())
	}

	ticker, ok := p.tickers[channel]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf("bitfinex failed to get ticker price for %s", cp.String())
	}

	return types.NewTickerPrice(
		string(ProviderBitfinex),
		cp.String(),
		fmt.Sprintf("%f", ticker.Price),
		fmt.Sprintf("%f", ticker.Volume),
		ticker.Time,
	)
}

func (p *BitfinexProvider) messageReceived(MessageType int, bz []byte) {
	var (
		response         [2]json.RawMessage
		tickerChannel    uint64
		tickerData       [10]float64
		heartbeat        string
		subscriptionResp BitfinexSubscriptionResponse
	)

	err := json.Unmarshal(bz, &subscriptionResp)
	if err == nil {
		p.channels[subscriptionResp.Pair] = subscriptionResp.ChannelID
		return
	}

	err = json.Unmarshal(bz, &response)
	if err == nil {

		err = json.Unmarshal(response[1], &heartbeat)
		if err == nil && heartbeat == "hb" {
			return
		}

		channelErr := json.Unmarshal(response[0], &tickerChannel)
		dataErr := json.Unmarshal(response[1], &tickerData)

		if channelErr == nil && dataErr == nil {
			p.setTickerPair(tickerChannel, tickerData)
			return
		}
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("ticker", err).
		Msg("Error on receive message")
}

func (p *BitfinexProvider) setTickerPair(channel uint64, data [10]float64) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.tickers[channel] = BitfinexTicker{
		Price:  data[6],
		Volume: data[7],
		Time:   time.Now().UnixMilli(),
	}
}

func (p *BitfinexProvider) GetAvailablePairs() (map[string]struct{}, error) {
	// not used yet, so skipping this unless needed
	return make(map[string]struct{}, 0), nil
}
