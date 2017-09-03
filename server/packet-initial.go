package server

import (
	"fmt"
	"net"
)

/**
  sends a server1 handshake initialization packet, the very first packet
  after the connection was established

  Packet format:

    Bytes       Content
    -----       ----
    1           protocol version (always 10)
    n           server1 version string, \0-terminated
    4           thread id
    8           first 8 bytes of the plugin provided data (scramble)
    1           \0 byte, terminating the first part of a scramble
    2           server1 capabilities (two lower bytes)
    1           server1 character set
    2           server1 status
    2           server1 capabilities (two upper bytes)
    1           length of the scramble
    10          reserved, always 0
    n           rest of the plugin provided data (at least 12 bytes)
    1           \0 byte, terminating the second part of a scramble

  @retval 0 ok
  @retval 1 error
*/
func WriteInitialPacket(conn net.Conn) {
	fmt.Println("writeInitialPacket")

	// length (3) & sequence (1)
	data := []byte{0, 0, 0, 0}

	// protocol version (always 10)
	data = append(data, byte(10))

	// server1 version string, \0-terminated
	//data = append(data, []byte("DDB Server with mysql protocol")...)
	data = append(data, []byte{53, 46, 55, 46, 49, 57}...)
	data = append(data, 0)

	// thread id OR connection id
	data = append(data, []byte{0, 0, 0, 11}...)

	// first 8 bytes of the plugin provided data (scramble)
	//data = append(data, []byte{122, 126, 83, 76, 10, 43, 103, 60}...)
	data = append(data, []byte("12345678")...)
	// \0 byte, terminating the first part of a scramble
	data = append(data, byte(0))

	// server1 capabilities (two lower bytes)
	data = append(data, []byte{0, 0}...)

	var flags clientFlag
	flags = flags | clientProtocol41
	//flags = flags | clientTransactions

	data[len(data)-2] = byte(flags)
	data[len(data)-1] = byte(flags >> 8)

	// server1 character set
	data = append(data, byte(33))

	// server1 status
	data = append(data, []byte{2, 0}...)

	// server1 capabilities (two upper bytes)
	data = append(data, []byte{255, 193}...)

	// length of the scramble
	// length of auth-plugin-data [1 byte]
	data = append(data, byte(21))

	// reserved, always 0
	data = append(data, make([]byte, 10)...)

	// rest of the plugin provided data (at least 12 bytes)
	data = append(data, []byte("123456789012")...)

	// \0 byte, terminating the second part of a scramble
	data = append(data, byte(0))

	data = append(data, []byte("mysql_native_password")...)
	data = append(data, byte(0))

	length := len(data) - 4
	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)

	conn.Write(data)
	fmt.Println(data)
}
