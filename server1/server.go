package server1

import (
	"net"
	"fmt"
	"os"
)

func AcceptConnections(host, port string, handleFunc func(conn net.Conn)) {

	l, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	defer l.Close()

	fmt.Println("Listening on " + host + ":" + port)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		} else {
			fmt.Println("accepted connection from", conn.RemoteAddr())
		}

		//go handleFunc(conn)
		go handleFunc(conn)
	}
}
