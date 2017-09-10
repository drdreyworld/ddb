package main

import (
	"ddb/server"
	"ddb/server/mysql41"
	"ddb/sql"
	"flag"
	"log"
	"net"
	"os"
)

var err error

var (
	Host *string = flag.String("host", "127.0.0.1", "server host")
	Port *string = flag.String("port", "3306", "server port")
)

func main() {
	flag.Parse()

	listener := server.Listener{Host: *Host, Port: *Port}
	listener.HandleFunc = func(conn net.Conn) {
		mysql41.NewConnection(conn).Handle(&sql.Parser{})
	}

	if err = listener.Listen(); err != nil {
		log.Fatalln(err)
		os.Exit(1)
	}
}
