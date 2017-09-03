package main

import (
	"ddb/server1"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

const maxPacketSize = 1<<24 - 1

func main() {
	server1.AcceptConnections("127.0.0.1", "3306", handleRequest)
}

func handleRequest(conn net.Conn) {
	defer func() {
		conn.Close()
		fmt.Println("Close connection")
	}()

	var sequence byte

	sequence = 1
	fmt.Println(sequence)

	var data []byte
	var err error

	server1.WriteInitialPacket(conn)

	if err = readAuthPacket(conn); err != nil {
		panic(err)
	}

	sequence++
	writeOkPacket(conn, sequence)

	for {
		sequence = 0

		if data, err = readBytes(conn); err != nil {
			fmt.Println("readBytes Error:", err)
			break
		}

		fmt.Println(data)

		switch readCommand(data) {
		case "select @@version_comment limit 1":
			//sequence++
			//writeResultSetHeaderPacket(conn, sequence, 1)
			//
			//sequence++
			//writeEofPacket(conn, sequence)
			//
			sequence++
			writeResultString(conn, sequence, "DDB sql server1 v1.0")
			//
			//sequence++
			//writeEofPacket(conn, sequence)

			break
		case "SELECT @@max_allowed_packet":
			sequence++
			writeResultSetHeaderPacket(conn, sequence, 1)

			sequence++
			writeEofPacket(conn, sequence)

			sequence++
			writeResultString(conn, sequence, strconv.Itoa(maxPacketSize))

			sequence++
			writeEofPacket(conn, sequence)

			break
		case "SELECT @@some":

			sequence++
			writeResultSetHeaderPacket(conn, sequence, 1)

			sequence++
			writeColumnPacket(conn, sequence, "@@some")

			sequence++
			writeEofPacket(conn, sequence)

			sequence++
			writeResultString(conn, sequence, "some fucked value")

			sequence++
			writeEofPacket(conn, sequence)

			break
		case "SELECT * FROM user":

			sequence++
			writeResultSetHeaderPacket(conn, sequence, 2)

			sequence++
			writeColumnPacket(conn, sequence, "FName")

			sequence++
			writeColumnPacket(conn, sequence, "LName")

			sequence++
			writeEofPacket(conn, sequence)

			sequence++
			writeResultStrings(conn, sequence, []string{
				"Вася",
				"Пупкин",
			})

			sequence++
			writeEofPacket(conn, sequence)

			break
		case "SELECT 'aaa'":
			fmt.Println("--------------------------")
			sequence++
			writeOkWithData(conn, sequence)
			break
		case "SELECT DATABASE()":
			sequence++
			writeResultString(conn, sequence, "USERS")

			break
		case "SET NAMES utf8":
			sequence++
			writeOkPacket(conn, sequence)

			break
		default:
			sequence++
			writeErrPacket(conn, sequence, 75, "Can't parse request")
			break
		}

		time.Sleep(time.Second)
	}

	//conn.Close()
}

func readBytes(conn net.Conn) ([]byte, error) {
	fmt.Println("readBytes")
	buf := make([]byte, 1024)

	//conn.SetReadDeadline(time.Now().Add(time.Second * 2))
	reqLen, err := conn.Read(buf)
	if err != nil {
		//fmt.Println("Error reading:", err.Error())
		return nil, err
	}

	fmt.Println("Recieved", reqLen, "bytes")
	return buf, nil
}

/**

0..3 - packet length
4..7 - client flags
8..11 - max packet size
12 - collation
13 + 23 - 0 - filter
~ 36 - User null terminated string
1 - length of scrambleBuff
X - scrambleBuff
Databasename [null terminated string]
mysql_native_password + 0x00

*/

var p authPacket

func readAuthPacket(conn net.Conn) error {
	fmt.Println("readAuthPacket")
	data := make([]byte, 1024)

	if _, err := conn.Read(data); err != nil {
		return err
	}

	fmt.Println(data)

	p = authPacket{
		length:          int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16),
		flags:           int(uint32(data[4]) | uint32(data[5])<<8 | uint32(data[6])<<16 | uint32(data[7])<<32),
		maxPacketLength: int(uint32(data[8]) | uint32(data[9])<<8 | uint32(data[10])<<16 | uint32(data[11])<<32),

		collation: data[12],
	}

	copy(p.filter, data[13:13+23])

	pos := 13 + 23
	i := pos
	for ; data[i] != 0; i++ {
	}

	p.user = string(data[pos:i])

	pos = i + 2
	i = pos
	for ; data[i] != 0; i++ {
	}

	p.database = string(data[pos:i])

	pos = i + 1
	fmt.Println(p)

	return nil
}

type authPacket struct {
	length          int
	flags           int
	maxPacketLength int
	collation       byte
	filter          []byte
	user            string
	database        string
}

const (
	iOK          byte = 0x00
	iLocalInFile byte = 0xfb
	iEOF         byte = 0xfe
	iERR         byte = 0xff
)

func writePacket(conn net.Conn, data []byte) error {
	length := len(data) - 4

	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)

	_, err := conn.Write(data)

	return err
}

func writeOkWithData(conn net.Conn, sequence byte) error {
	fmt.Println("writeOkWithData")

	fmt.Println(p)

	// 97 110 100 114 101 121 0 0
	data := []byte{
		1, 0, 0, 1, 1,
		25, 0, 0, 2, 3,
		//100, 101, 102,
		0, 0, 0,
		0, 0, 0, 3, 97, 97, 97, 0, 12, 33, 0,
		9, 0, 0, 0, 253, 1, 0, 31, 0, 0, 5, 0, 0, 3, 254, 0, 0, 2,

		0, 4, 0, 0, 4, 3, 97, 97, 97, 5, 0, 0, 5, 254, 0, 0, 2, 0,
	}
	fmt.Println(data)
	_, err := conn.Write(data)
	return err
}



func writeOkPacket(conn net.Conn, sequence byte) error {
	fmt.Println("writeOkPacket")

	affectedRows := uint64(0)
	lastInsertId := uint64(0)

	data := []byte{0, 0, 0, sequence, iOK}
	data = appendLengthEncodedInteger(data, affectedRows)
	data = appendLengthEncodedInteger(data, lastInsertId)
	data = append(data, []byte{0,0}...) // status
	data = append(data, []byte{0,0}...) // warnings

	return writePacket(conn, data)
}


func writeOk(conn net.Conn, sequence byte, affectedRows, lastInsertId uint64, status statusFlag) error {
	fmt.Println("writeOkPacket")

	data := []byte{0, 0, 0, sequence, iOK}
	data = appendLengthEncodedInteger(data, affectedRows)
	data = appendLengthEncodedInteger(data, lastInsertId)

	data = append(data, []byte{
		byte(status),
		byte(status >> 8),
	}...) // status

	data = append(data, []byte{0,0}...) // warnings

	return writePacket(conn, data)
}

func writeEofPacket(conn net.Conn, sequence byte) error {
	fmt.Println("writeEofPacket")

	data := []byte{0, 0, 0, sequence, iEOF}
	data = append(data, []byte{0,0}...) // warnings
	data = append(data, []byte{0,0}...) // status

	// https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#a1d854e841086925be1883e4d7b4e8cad

	return writePacket(conn, data)
}

func writeErrPacket(conn net.Conn, sequence byte, code int, message string) error {
	fmt.Println("writeErrPacket")

	data := []byte{0, 0, 0, sequence, iERR}

	data = append(data, []byte{
		byte(code),
		byte(code >> 8),
	}...)

	// flag for sql state #
	data = append(data, 0x23)
	//sqlstate := string(data[4 : 4+5])
	//@todo get sql states
	data = append(data, []byte{4, 1, 0, 0, 0}...)

	data = append(data, []byte(message)...)
	data = append(data, byte(0))

	return writePacket(conn, data)
}

func writeResultString(conn net.Conn, sequence byte, result string) error {
	fmt.Println("writeResultString")

	data := []byte{1, 0, 0, sequence}
	data = writeLengthEncodedString(data, result)

	_, err := conn.Write(data)
	return err
	//return writePacket(conn, data)
}

func writeResultStrings(conn net.Conn, sequence byte, result []string) error {
	fmt.Println("writeResultString")

	data := []byte{0, 0, 0, sequence}

	for _, v := range result {
		data = writeLengthEncodedString(data, v)
	}

	return writePacket(conn, data)
}

type column struct {
	catalog       string
	database      string
	originalTable string
	name          string
}

func writeColumnPacket(conn net.Conn, sequence byte, column string) error {
	fmt.Println("writeColumnPacket")

	data := []byte{0, 0, 0, sequence}

	data = writeLengthEncodedString(data,"")
	data = writeLengthEncodedString(data,"DBPassport")
	data = writeLengthEncodedString(data,"TabUsers")
	data = writeLengthEncodedString(data,"")
	data = writeLengthEncodedString(data, column)
	data = append(data, byte(0))

	// Filler [uint8]
	// Charset [charset, collation uint8]
	// Length [uint32]
	// Type byte
	// Flags [uint16]
	// Decimals [uint8]

	data = append(data, []byte{
		0,
		33, 1,
		50, 0, 0, 0,
		//fieldTypeVarChar,
		fieldTypeDateTime,
		0, 0, //flags
		0, // Decimals [uint8]
	}...)

	// Default
	//data = writeLengthEncodedString(data,"some default value")
	//data = append(data, byte(0))

	return writePacket(conn, data)
}

func writeLengthEncodedString(data []byte, value string) []byte {
	data = appendLengthEncodedInteger(data, uint64(len(value)))
	data = append(data, []byte(value)...)

	return data
}

func appendLengthEncodedInteger(b []byte, n uint64) []byte {
	switch {
	case n <= 250:
		return append(b, byte(n))
	case n <= 0xffff:
		return append(b, 0xfc, byte(n), byte(n>>8))
	case n <= 0xffffff:
		return append(b, 0xfd, byte(n), byte(n>>8), byte(n>>16))
	}
	return append(b, 0xfe, byte(n), byte(n>>8), byte(n>>16), byte(n>>24),
		byte(n>>32), byte(n>>40), byte(n>>48), byte(n>>56))
}


func writeResultSetHeaderPacket(conn net.Conn, sequence byte, colcount uint64) error {
	fmt.Println("writeResultSetHeaderPacket")
	data := []byte{0, 0, 0, sequence}
	data = appendLengthEncodedInteger(data, colcount)

	fmt.Println("Multicolcount:", data)
	return writePacket(conn, data)
}

func writeResultSetColumnsPacket(conn net.Conn, sequence byte, result []byte) error {
	fmt.Println("writeResultSetColumnsPacket")

	rowcount := byte(1)

	data := []byte{0, 0, 0, sequence, rowcount}
	data = append(data, result...)
	data = append(data, byte(0))

	return writePacket(conn, data)
}

func readCommand(data []byte) string {

	// packet length [24 bit]
	length := int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16)
	sequence := data[3]

	command := data[5 : 4+length]

	fmt.Println("length:", length, "sequence:", sequence)

	result := strings.Trim(string(command), " ")

	fmt.Println("query:", result)
	return result
}

// https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType
const (
	fieldTypeDecimal byte = iota
	fieldTypeTiny
	fieldTypeShort
	fieldTypeLong
	fieldTypeFloat
	fieldTypeDouble
	fieldTypeNULL
	fieldTypeTimestamp
	fieldTypeLongLong
	fieldTypeInt24
	fieldTypeDate
	fieldTypeTime
	fieldTypeDateTime
	fieldTypeYear
	fieldTypeNewDate
	fieldTypeVarChar
	fieldTypeBit
)
const (
	fieldTypeJSON byte = iota + 0xf5
	fieldTypeNewDecimal
	fieldTypeEnum
	fieldTypeSet
	fieldTypeTinyBLOB
	fieldTypeMediumBLOB
	fieldTypeLongBLOB
	fieldTypeBLOB
	fieldTypeVarString
	fieldTypeString
	fieldTypeGeometry
)
type fieldFlag uint16

const (
	flagNotNULL fieldFlag = 1 << iota
	flagPriKey
	flagUniqueKey
	flagMultipleKey
	flagBLOB
	flagUnsigned
	flagZeroFill
	flagBinary
	flagEnum
	flagAutoIncrement
	flagTimestamp
	flagSet
	flagUnknown1
	flagUnknown2
	flagUnknown3
	flagUnknown4
)

// http://dev.mysql.com/doc/internals/en/status-flags.html
type statusFlag uint16

const (
	statusInTrans statusFlag = 1 << iota
	statusInAutocommit
	statusReserved // Not in documentation
	statusMoreResultsExists
	statusNoGoodIndexUsed
	statusNoIndexUsed
	statusCursorExists
	statusLastRowSent
	statusDbDropped
	statusNoBackslashEscapes
	statusMetadataChanged
	statusQueryWasSlow
	statusPsOutParams
	statusInTransReadonly
	statusSessionStateChanged
)
