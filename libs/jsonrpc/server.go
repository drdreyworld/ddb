package jsonrpc

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type JsonRpcServerHandler func(request JsonRpcRequest) (interface{}, error)

type JsonRpcServer struct {
	Host string `json:"host"`
	Port int    `json:"port"`

	Server  *http.Server
	handler JsonRpcServerHandler
}

func (self *JsonRpcServer) SetHandler(handler JsonRpcServerHandler) {
	self.handler = handler
}

func (self *JsonRpcServer) ListenAndServe() {
	self.Server = &http.Server{
		Addr:           fmt.Sprintf("%s:%d", self.Host, self.Port),
		Handler:        self,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	self.Server.ListenAndServe()
}

func (self *JsonRpcServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	response := &JsonRpcResponse{
		JsonRpc: "2.0",
	}

	if body, err := ioutil.ReadAll(r.Body); err == nil {
		defer r.Body.Close()

		var request JsonRpcRequest

		if err = json.Unmarshal(body, &request); err == nil {
			if self.handler != nil {
				if result, err := self.handler(request); err == nil {
					response.Result = result
					if request.Id > 0 {
						response.Id = request.Id + 1
					}
				} else {
					response.Error.Code = -32601
					response.Error.Message = "Procedure not found"
				}
			} else {
				response.Error.Code = -32601
				response.Error.Message = "JSON-RPC Handler not initialized"
			}
		} else {
			response.Error.Code = -32600
			response.Error.Message = "Invalid JSON-RPC"
		}
	} else {
		response.Error.Code = -32700
		response.Error.Message = "Parse error"
	}

	if resp, err := json.Marshal(response); err == nil {
		w.Write(resp)
		if response.Error.Code < 0 {
			log.Println("json-rpc error code:", response.Error.Code, " message: ", response.Error.Message)
		}
	} else {
		log.Fatal(err)
	}
}
