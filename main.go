package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"flag"
	"ddb/storage"
	"ddb/protocol/mysql41"
	"net"
	"ddb/types/queryparser"
	"os"
	"time"
)

var (
	Host *string = flag.String("host", "127.0.0.1", "server host")
	Port *string = flag.String("port", "3306", "server port")
)

func init() {
	flag.Parse()
}

func profilerStart() {
	log.Println(http.ListenAndServe("localhost:6060", nil))
}

func main() {
	go profilerStart()

	tableManager := &storage.TableManager{}
	tableManager.Init()

	queryProcessor := &storage.QueryProcessor{}
	queryProcessor.Init(tableManager)

	mysql41Lisneter := mysql41.Listener{Name: "mysql41", Host:*Host, Port:*Port}
	mysql41Lisneter.HandleFunc = func(conn net.Conn) {
		mysql41.NewConnection(conn).Handle(&queryparser.Parser{}, queryProcessor)
	}

	go func() {
		if err := mysql41Lisneter.Listen(); err != nil {
			log.Fatalln(err)
			os.Exit(1)
		}
	}()

	for {
		//fmt.Print("\rddb server running: ", time.Now())
		time.Sleep(time.Second)
	}
}
