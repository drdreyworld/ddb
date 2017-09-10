package protocol

import (
	"fmt"
	"net"
)

type Listener struct {
	host     string
	port     string
	listener net.Listener
}

func NewListener(host, port string) *Listener {
	result := &Listener{
		host: host,
		port: port,
	}
	return result
}

func (self *Listener) Println(a ...interface{}) {
	fmt.Println(a...)
}

func (self *Listener) Listen() (err error) {
	self.listener, err = net.Listen("tcp", self.host+":"+self.port)
	if err != nil {
		return err
	}

	self.Println("listening on " + self.host + ":" + self.port)
	return nil
}

func (self *Listener) CloseConnection() {
	self.listener.Close()
}

func (self *Listener) AcceptConnections() (err error) {
	for {
		conn := Connection{}

		conn.listener = self
		conn.connection, err = self.listener.Accept()

		self.Println("accept connection from:", conn.connection.RemoteAddr())

		if err != nil {
			return err
		}

		go conn.Handle()
	}
}