package types

type (
	message struct {
		Type string `json:"@type"`
	}

	body struct {
		Messages []message `json:"messages"`
	}

	tx struct {
		Body body `json:"body"`
	}

	header struct {
		Height string `json:"height"`
		Time   string `json:"time"`
	}

	data struct {
		Txs []string `json:"txs"`
	}

	block struct {
		Header header `json:"header"`
		Data   data   `json:"data"`
	}

	CosmosBlockResponse struct {
		Txs   []tx  `json:"txs"`
		Block block `json:"block"`
	}
)
