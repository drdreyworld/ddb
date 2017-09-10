package server

import (
	"fmt"
	"net"
)

type HandleFunc func(conn net.Conn)

type Listener struct {
	Host string
	Port string

	HandleFunc HandleFunc

	listener net.Listener
}

func (self *Listener) Listen() (err error) {

	self.listener, err = net.Listen("tcp", self.Host+":"+self.Port)

	if err != nil {
		return err
	}

	defer self.listener.Close()

	fmt.Println("listening on " + self.Host + ":" + self.Port)

	for {
		conn, err := self.listener.Accept()

		if err != nil {
			return err
		}

		go self.HandleFunc(conn)
	}
}
