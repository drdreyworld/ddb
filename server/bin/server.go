package main

import (
	"ddb/server"
	"ddb/server/mysql41"
	"flag"
	"log"
	"net"
	"os"
	_ "net/http/pprof"
	"net/http"
	"ddb/types/queryprocessor"
	"ddb/types/queryparser"
)

var err error

var (
	Host *string = flag.String("host", "127.0.0.1", "server host")
	Port *string = flag.String("port", "3306", "server port")
)

var queryProcessor *queryprocessor.QueryProcessor

func main() {
	flag.Parse()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	queryProcessor = &queryprocessor.QueryProcessor{}
	queryProcessor.Init()

	defer queryProcessor.CloseTables()

	listener := server.Listener{Host: *Host, Port: *Port}
	listener.HandleFunc = func(conn net.Conn) {
		mysql41.NewConnection(conn).Handle(&queryparser.Parser{}, queryProcessor)
	}

	if err = listener.Listen(); err != nil {
		log.Fatalln(err)
		os.Exit(1)
	}
}
