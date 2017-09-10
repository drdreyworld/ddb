package main

import (
	"ddb/server"
	"log"
	"os"
	"net"
	"ddb/server/mysql41"
	"ddb/sql"
)

var err error

func main() {
	listener := server.Listener{Host:"127.0.0.1", Port:"3306"}
	listener.HandleFunc = func(conn net.Conn) {
		mysql41.NewConnection(conn).Handle(&sql.Parser{})
	}

	if err = listener.Listen(); err != nil {
		log.Fatalln(err)
		os.Exit(1)
	}
}