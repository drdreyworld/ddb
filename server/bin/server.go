package main

import (
	"ddb/server/protocol"
	"log"
	"os"
)

var err error

func main() {
	listener := protocol.NewListener("127.0.0.1", "3306")

	if err = listener.Listen(); err != nil {
		log.Fatalln(err)
		os.Exit(1)
	}

	defer listener.CloseConnection()

	if err = listener.AcceptConnections(); err != nil {
		log.Fatalln(err)
		os.Exit(1)
	}
}