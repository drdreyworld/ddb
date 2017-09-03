package jsonrpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type JsonRpcClient struct {
	Host string
	Port int
}

func (self *JsonRpcClient) Call(method string, params interface{}, result interface{}) error {

	request_str, err := json.Marshal(JsonRpcRequest{
		JsonRpc: "2.0",
		Method:  method,
		Params:  params,
	})

	if err != nil {
		log.Println(err)
		return err
	}

	url := fmt.Sprintf("http://%s:%d/", self.Host, self.Port)

	r, err := http.NewRequest("POST", url, bytes.NewBuffer(request_str))

	client := &http.Client{
		Timeout: 2 * time.Second,
	}

	response, err := client.Do(r)

	if err != nil {
		log.Println(err)
		return err
	}

	defer response.Body.Close()

	content, err := ioutil.ReadAll(response.Body)

	if err != nil {
		log.Println(err)
		return err
	}

	if err := json.Unmarshal(content, &result); err != nil {
		log.Println(err)
		return err
	}

	if r, ok := result.(JsonRpcResponse); ok && r.Error.Code < 0 {
		log.Println(r.Error.Message)
	}

	return nil
}
