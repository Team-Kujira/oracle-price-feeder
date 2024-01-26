package provider

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
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
		tokens    map[string]ShadeToken
	}

	ShadeToken struct {
		Decimals int64
		Address  string
		Hash     string
	}

	ShadePairInfoResponse struct {
		PairInfo ShadePairInfo `json:"get_pair_info"`
	}

	ShadePairInfo struct {
		Pair [2]ShadePair `json:"pair"`
	}

	ShadePair struct {
		CustomToken ShadeCustomToken `json:"custom_token"`
	}

	ShadeCustomToken struct {
		Contract string `json:"contract_addr"`
		Hash     string `json:"token_code_hash"`
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

		base, found := p.tokens[pair.Base]
		if !found {
			continue
		}

		quote, found := p.tokens[pair.Quote]
		if !found {
			continue
		}

		amount := int64(math.Pow10(int(base.Decimals)))

		message := fmt.Sprintf(`{
			"swap_simulation": {
				"offer": {
					"token": {
						"custom_token": {
							"contract_addr": "%s",
							"token_code_hash": "%s"
						}
					},
					"amount":"%d"
				},
				"exclude_fee": true
			}
		}`, base.Address, base.Hash, amount)

		content, err := p.query(contract, hash, message)
		if err != nil {
			p.logger.Err(err).Msg("")
			continue
		}

		var response struct {
			Simulation struct {
				Price string `json:"price"`
			} `json:"swap_simulation"`
		}
		err = json.Unmarshal(content, &response)
		if err != nil {
			p.logger.Err(err).Msg("")
			continue
		}

		price := strToDec(response.Simulation.Price)

		factor, err := computeDecimalsFactor(base.Decimals, quote.Decimals)
		if err != nil {
			continue
		}

		price = price.Mul(factor)

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
	p.tokens = map[string]ShadeToken{}

	for _, pair := range p.getAllPairs() {
		contract, err := p.getContractAddress(pair)
		if err != nil {
			continue
		}

		hash, err := p.getCodeHash(contract)
		if err != nil {
			continue
		}
		p.hashes[contract] = hash

		tokens, err := p.getPairInfo(contract, hash)
		if err != nil {
			continue
		}

		denoms := []string{
			pair.Base,
			pair.Quote,
		}

		for i, token := range tokens {
			denom := denoms[i]
			p.tokens[denom], err = p.getTokenInfo(token.Contract, token.Hash)
			if err != nil {
				continue
			}
		}
	}
}

func (p *ShadeProvider) query(
	contract string,
	codeHash string,
	message string,
) ([]byte, error) {
	message, err := p.compactJsonString(message)
	if err != nil {
		return nil, err
	}

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

	var response struct {
		Data string `json:"data"`
	}
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

	var response struct {
		CodeHash string `json:"code_hash"`
	}
	err = json.Unmarshal(content, &response)
	if err != nil {
		p.logger.Err(err).Msg("")
		return "", err
	}

	return response.CodeHash, nil
}

func (p *ShadeProvider) getTokenInfo(contract, hash string) (ShadeToken, error) {
	var token ShadeToken

	content, err := p.query(contract, hash, `{"token_info":{}}`)
	if err != nil {
		return token, err
	}

	var response struct {
		TokenInfo struct {
			Decimals int64 `json:"decimals"`
		} `json:"token_info"`
	}

	err = json.Unmarshal(content, &response)
	if err != nil {
		p.logger.Err(err).Msg("")
		return token, err
	}

	decimals := response.TokenInfo.Decimals

	if decimals < 1 {
		err = fmt.Errorf("decimal not found")
		p.logger.Error().Err(err).Msg("")
		return token, err
	}

	token = ShadeToken{
		Decimals: decimals,
		Address:  contract,
		Hash:     hash,
	}

	return token, nil
}

func (p *ShadeProvider) getPairInfo(
	contract string,
	hash string,
) ([]ShadeCustomToken, error) {
	tokens := make([]ShadeCustomToken, 2)

	content, err := p.query(contract, hash, `{"get_pair_info":{}}`)
	if err != nil {
		p.logger.Err(err).Msg("")
		return tokens, err
	}

	var response ShadePairInfoResponse
	err = json.Unmarshal(content, &response)
	if err != nil {
		p.logger.Err(err).Msg("")
		return nil, err
	}

	for i := 0; i < 2; i++ {
		tokens[i] = response.PairInfo.Pair[i].CustomToken
	}

	return tokens, nil
}
