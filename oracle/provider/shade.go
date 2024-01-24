package provider

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"time"

	"price-feeder/oracle/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/miscreant/miscreant.go"
	"github.com/rs/zerolog"
	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/hkdf"
)

var (
	_ Provider = (*ShadeProvider)(nil)

	shadeDefaultEndpoints = Endpoint{
		Name:         ProviderShade,
		Urls:         []string{},
		PollInterval: 6 * time.Second,
		// ContractAddresses: map[string]string{},
	}
)

type (
	// Secret tokens:
	// https://docs.scrt.network/secret-network-documentation/development/resources-api-contract-addresses/secret-token-contracts
	//
	// Shade defines an oracle provider that uses the API of an Secret Network
	// node to directly retrieve the price from the fin contract
	ShadeProvider struct {
		provider
		contracts map[string]string
		privKey   [32]byte
		pubKey    [32]byte
		nonce     []byte
		cipher    *miscreant.Cipher
		hashes    map[string]string
		decimals  map[string]uint64
	}

	ShadeCodeHashResponse struct {
		CodeHash string `json:"code_hash"`
	}

	ShadeResponse struct {
		Data string `json:"data"`
	}

	ShadePairInfoResponse struct {
		PairInfo ShadePairInfo `json:"get_pair_info"`
	}

	ShadePairInfo struct {
		Amount0 string       `json:"amount_0"`
		Amount1 string       `json:"amount_1"`
		Pair    [2]ShadePair `json:"pair"`
	}

	ShadePair struct {
		CustomToken ShadeCustomToken `json:"custom_token"`
	}

	ShadeCustomToken struct {
		Contract string `json:"contract_addr"`
		Hash     string `json:"token_code_hash"`
	}

	ShadeTokenInfoResponse struct {
		TokenInfo ShadeTokenInfo `json:"token_info"`
	}

	ShadeTokenInfo struct {
		Decimals uint64 `json:"decimals"`
	}
)

func NewShadeProvider(
	ctx context.Context,
	logger zerolog.Logger,
	endpoints Endpoint,
	pairs ...types.CurrencyPair,
) (*ShadeProvider, error) {
	provider := &ShadeProvider{}
	provider.Init(
		ctx,
		endpoints,
		logger,
		pairs,
		nil,
		nil,
	)

	provider.contracts = provider.endpoints.ContractAddresses

	availablePairs, _ := provider.GetAvailablePairs()
	provider.setPairs(pairs, availablePairs, nil)

	provider.init()

	go startPolling(provider, provider.endpoints.PollInterval, logger)
	return provider, nil
}

func (p *ShadeProvider) Poll() error {
	timestamp := time.Now()

	p.mtx.Lock()
	defer p.mtx.Unlock()

	for symbol, pair := range p.getAllPairs() {

		contract, err := p.getContractAddress(pair)
		if err != nil {
			p.logger.Warn().
				Str("symbol", symbol).
				Msg("no contract address found")
			continue
		}

		hash, found := p.hashes[contract]
		if !found {
			continue
		}

		content, err := p.query(contract, hash, `{"get_pair_info":{}}`)
		if err != nil {
			p.logger.Err(err).Msg("")
			continue
		}

		var response ShadePairInfoResponse
		err = json.Unmarshal(content, &response)
		if err != nil {
			p.logger.Err(err).Msg("")
			continue
		}

		token0 := response.PairInfo.Pair[0].CustomToken
		token1 := response.PairInfo.Pair[1].CustomToken

		decimals0, err := p.getDecimals(token0.Contract, token0.Hash)
		if err != nil {
			continue
		}

		decimals1, err := p.getDecimals(token1.Contract, token1.Hash)
		if err != nil {
			continue
		}

		amount0 := strToDec(response.PairInfo.Amount0)
		amount1 := strToDec(response.PairInfo.Amount1)

		amount0 = amount0.Quo(uintToDec(10).Power(decimals0))
		amount1 = amount1.Quo(uintToDec(10).Power(decimals1))

		price := amount1.Quo(amount0)

		p.setTickerPrice(
			symbol,
			price,
			sdk.ZeroDec(),
			timestamp,
		)
	}

	return nil
}

func (p *ShadeProvider) GetAvailablePairs() (map[string]struct{}, error) {
	return p.getAvailablePairsFromContracts()
}

func (p *ShadeProvider) init() {
	rand.Read(p.privKey[:])

	curve25519.ScalarBaseMult(&p.pubKey, &p.privKey)

	p.nonce = make([]byte, 32)
	_, err := rand.Read(p.nonce)
	if err != nil {
		p.logger.Err(err).Msg("")
		panic(err)
	}

	consensusPub, err := base64.StdEncoding.DecodeString(
		"79++5YOHfm0SwhlpUDClv7cuCjq9xBZlWqSjDJWkRG8=",
	)
	if err != nil {
		p.logger.Err(err).Msg("")
		panic(err)
	}

	sharedSecret, err := curve25519.X25519(p.privKey[:], consensusPub)
	if err != nil {
		p.logger.Err(err).Msg("")
		panic(err)
	}

	hkdfSalt := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x02, 0x4b, 0xea, 0xd8, 0xdf, 0x69, 0x99,
		0x08, 0x52, 0xc2, 0x02, 0xdb, 0x0e, 0x00, 0x97,
		0xc1, 0xa1, 0x2e, 0xa6, 0x37, 0xd7, 0xe9, 0x6d,
	}

	hkdfReader := hkdf.New(
		sha256.New,
		append(sharedSecret, p.nonce...),
		hkdfSalt,
		[]byte{},
	)

	encryptionKey := make([]byte, 32)
	_, err = io.ReadFull(hkdfReader, encryptionKey)
	if err != nil {
		p.logger.Err(err).Msg("")
		panic(err)
	}

	p.cipher, err = miscreant.NewAESCMACSIV(encryptionKey)
	if err != nil {
		p.logger.Err(err).Msg("")
		panic(err)
	}

	p.hashes = map[string]string{}

	for _, contract := range p.contracts {
		hash, err := p.getCodeHash(contract)
		if err != nil {
			p.logger.Error().Err(err).Msg("")
		}
		p.hashes[contract] = hash
	}

	p.decimals = map[string]uint64{}
}

func (p *ShadeProvider) query(
	contract string,
	codeHash string,
	message string,
) ([]byte, error) {
	plaintext := codeHash + message

	ciphertext, err := p.cipher.Seal(nil, []byte(plaintext), []byte{})
	if err != nil {
		p.logger.Err(err).Msg("")
		return nil, err
	}

	bz := append(p.nonce, append(p.pubKey[:], ciphertext...)...)

	query := base64.StdEncoding.EncodeToString(bz)
	query = url.QueryEscape(query)

	path := fmt.Sprintf(
		"/compute/v1beta1/query/%s?query=%s",
		contract, query,
	)

	content, err := p.httpGet(path)
	if err != nil {
		p.logger.Err(err).Msg("")
		return nil, err
	}

	var response ShadeResponse
	err = json.Unmarshal(content, &response)
	if err != nil {
		p.logger.Err(err).Msg("")
		return nil, err
	}

	data, err := base64.StdEncoding.DecodeString(response.Data)
	if err != nil {
		p.logger.Err(err).Msg("")
		return nil, err
	}

	decrypted, err := p.cipher.Open(nil, data, []byte{})
	if err != nil {
		p.logger.Err(err).Msg("")
		return nil, err
	}

	// Decode base64 string to get the original byte slice.
	decoded, err := base64.StdEncoding.DecodeString(string(decrypted))
	if err != nil {
		p.logger.Err(err).Msg("")
		return nil, err
	}

	return decoded, nil
}

func (p *ShadeProvider) getCodeHash(contract string) (string, error) {
	path := fmt.Sprintf(
		"/compute/v1beta1/code_hash/by_contract_address/%s",
		contract,
	)

	content, err := p.httpGet(path)
	if err != nil {
		p.logger.Error().Msg("failed getting code hash")
		return "", err
	}

	var response ShadeCodeHashResponse
	err = json.Unmarshal(content, &response)
	if err != nil {
		return "", err
	}

	return response.CodeHash, nil
}

func (p *ShadeProvider) getDecimals(contract, hash string) (uint64, error) {
	decimals, found := p.decimals[contract]
	if found {
		return decimals, nil
	}

	content, err := p.query(contract, hash, `{"token_info":{}}`)
	if err != nil {
		return 0, err
	}

	var response ShadeTokenInfoResponse

	err = json.Unmarshal(content, &response)
	if err != nil {
		return 0, err
	}

	decimals = response.TokenInfo.Decimals

	if decimals < 1 {
		err = fmt.Errorf("decimal not found")
		p.logger.Error().Err(err).Msg("")
		return 0, err
	}

	p.decimals[contract] = decimals

	return decimals, nil
}
