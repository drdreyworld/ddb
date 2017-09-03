package jsonrpc

type JsonRpcResponse struct {
	JsonRpc string       `json:"jsonrpc"`
	Result  interface{}  `json:"result"`
	Id      int          `json:"id"`
	Error   JsonRpcError `json:"error"`
}

type JsonRpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
