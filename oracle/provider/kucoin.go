package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"price-feeder/oracle/types"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
)

const (
	kucoinWSHost   = "ws-api-spot.kucoin.com"
	kucoinWSPath   = "/endpoint"
	kucoinRestHost = "api.kucoin.com"
)

var _ Provider = (*KucoinProvider)(nil)

type (
	KucoinProvider struct {
		wsc             *WebsocketController
		logger          zerolog.Logger
		mtx             sync.Mutex
		endpoints       Endpoint
		tickers         map[string]KucoinSnapshotDataData
		subscribedPairs map[string]types.CurrencyPair
	}

	KucoinSubscriptionMsg struct {
		ID    uint64 `json:"id"`
		Type  string `json:"type"`
		Topic string `json:"topic"`
	}

	KucoinSnapshotMsg struct {
		Topic   string             `json:"topic"`
		Subject string             `json:"subject"`
		Data    KucoinSnapshotData `json:"data"`
	}

	KucoinSnapshotData struct {
		Data KucoinSnapshotDataData `json:"data"`
	}

	KucoinSnapshotDataData struct {
		Base   string  `json:"baseCurrency"`
		Quote  string  `json:"quoteCurrency"`
		Symbol string  `json:"symbol"`
		Price  float64 `json:"lastTradedPrice"`
		Volume float64 `json:"vol"`
		Time   int64   `json:"datetime"`
	}

	KucoinTokenApiResponse struct {
		Data KucoinTokenApiData `json:"data"`
	}

	KucoinTokenApiData struct {
		Token   string                         `json:"token"`
		Servers []KucoinTokenApiInstanceServer `json:"instanceServers"`
	}

	KucoinTokenApiInstanceServer struct {
		Endpoint     string `json:"endpoint"`
		Protocol     string `json:"protocol"`
		PingInterval uint64 `json:"pingInterval"`
		PingTimeout  uint64 `json:"pingTimeout"`
	}
)

func NewKucoinProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*KucoinProvider, error) {
	if endpoints.Name != ProviderKucoin {
		endpoints = Endpoint{
			Name:      ProviderKucoin,
			Rest:      kucoinRestHost,
			Websocket: kucoinWSHost,
		}
	}

	// get public token
	resp, err := http.Post(
		"https://"+endpoints.Rest+"/api/v1/bullet-public",
		"application/json",
		bytes.NewBufferString(""),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var tokenResponse KucoinTokenApiResponse
	err = json.NewDecoder(resp.Body).Decode(&tokenResponse)
	if err != nil {
		return nil, err
	}

	wsURL := url.URL{
		Scheme: "wss",
		Host:   endpoints.Websocket,
		Path:   kucoinWSPath,
	}

	query := wsURL.Query()
	query.Set("token", tokenResponse.Data.Token)

	wsURL.RawQuery = query.Encode()

	kucoinLogger := logger.With().Str("provider", string(ProviderKucoin)).Logger()

	provider := &KucoinProvider{
		logger:          kucoinLogger,
		endpoints:       endpoints,
		tickers:         map[string]KucoinSnapshotDataData{},
		subscribedPairs: map[string]types.CurrencyPair{},
	}

	setSubscribedPairs(provider, pairs...)

	provider.wsc = NewWebsocketController(
		ctx,
		ProviderKucoin,
		wsURL,
		provider.GetSubscriptionMsgs(pairs...),
		provider.messageReceived,
		defaultPingDuration,
		websocket.TextMessage,
		kucoinLogger,
	)

	go provider.wsc.Start()

	return provider, nil
}

func (p *KucoinProvider) GetSubscriptionMsgs(cps ...types.CurrencyPair) []interface{} {
	subscriptionMsgs := make([]interface{}, len(cps))

	symbols := make([]string, len(cps))

	for i, cp := range cps {
		symbols[i] = cp.Join("-")
	}

	subscriptionMsgs[0] = KucoinSubscriptionMsg{
		ID:    1,
		Type:  "subscribe",
		Topic: "/market/snapshot:" + strings.Join(symbols, ","),
	}

	return subscriptionMsgs
}

func (p *KucoinProvider) GetSubscribedPair(s string) (types.CurrencyPair, bool) {
	cp, ok := p.subscribedPairs[s]
	return cp, ok
}

func (p *KucoinProvider) SetSubscribedPair(cp types.CurrencyPair) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.subscribedPairs[cp.String()] = cp
}

func (p *KucoinProvider) SubscribeCurrencyPairs(cps ...types.CurrencyPair) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return subscribeCurrencyPairs(p, cps)
}

func (p *KucoinProvider) SendSubscriptionMsgs(msgs []interface{}) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return p.wsc.AddSubscriptionMsgs(msgs)
}

func (p *KucoinProvider) GetTickerPrices(cps ...types.CurrencyPair) (map[string]types.TickerPrice, error) {
	return getTickerPrices(p, cps)
}

func (p *KucoinProvider) GetTickerPrice(cp types.CurrencyPair) (types.TickerPrice, error) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	key := cp.String()

	ticker, ok := p.tickers[key]
	if !ok {
		return types.TickerPrice{}, fmt.Errorf("kucoin failed to get ticker price for %s", key)
	}

	return types.NewTickerPrice(
		string(ProviderKucoin),
		key,
		fmt.Sprintf("%f", ticker.Price),
		fmt.Sprintf("%f", ticker.Volume),
		ticker.Time,
	)
}

func (p *KucoinProvider) messageReceived(messageType int, bz []byte) {
	var (
		snapshotMsg KucoinSnapshotMsg
		snapshotErr error
	)

	snapshotErr = json.Unmarshal(bz, &snapshotMsg)
	if snapshotErr == nil {
		p.setTickerPair(snapshotMsg.Data.Data)
		telemetryWebsocketMessage(ProviderKucoin, MessageTypeTicker)
		return
	}

	p.logger.Error().
		Int("length", len(bz)).
		AnErr("snapshot", snapshotErr).
		Msg("Error on receive message")
}

func (p *KucoinProvider) setTickerPair(data KucoinSnapshotDataData) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.tickers[data.Base+data.Quote] = data
}

func (p *KucoinProvider) GetAvailablePairs() (map[string]struct{}, error) {
	// not used yet, so skipping this unless needed
	return make(map[string]struct{}, 0), nil
}
