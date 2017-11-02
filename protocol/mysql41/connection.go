package mysql41

import (
	"ddb/types/queryparser"
	"ddb/storage"
	"ddb/types/rowset"
	"net"
	"log"
)

type Connection struct {
	connection    net.Conn
	sequence      byte
	status        statusFlag
	affecterRows  int
	lastInsertId  int
	warningsCount int
}

func NewConnection(conn net.Conn) *Connection {
	result := &Connection{}
	result.connection = conn
	return result
}

func (c *Connection) resetSequence() byte {
	c.sequence = 0
	return c.sequence
}

func (c *Connection) nextSequence() byte {
	c.sequence++
	return c.sequence
}

var err error

func (c *Connection) resetConnStatus() {
	c.status = 0
	c.affecterRows = 0
	c.lastInsertId = 0
	c.warningsCount = 0
}

func (c *Connection) Handle(parser *queryparser.Parser, processor *storage.QueryProcessor) {
	var err error
	var rows *rowset.Rowset

	c.resetConnStatus()
	c.writeInitialPacket()

	if err = c.readAuthPacket(); err != nil {
		panic(err)
	}

	c.nextSequence()
	c.writeOkPacket()

	for {
		p, err := c.readPacket()
		if err != nil {
			return
		}

		log.Println("query:", p.readQuery())

		if p.readQuery() == "SET NAMES utf8" {
			c.writeOkPacket()
			continue
		}

		if p.readQuery() == "SELECT DATABASE()" {
			c.writeOkPacket()
			continue
		}

		query, err := parser.Parse(p.readQuery())
		if err != nil {
			c.sequence = p.readSequence()
			c.writeError(1064, "Parse error:"+err.Error())
			continue
		}

		if query == nil {
			c.sequence = p.readSequence()
			c.writeError(1064, "Parse error: query is nil")
			continue
		}

		rows, c.affecterRows, err = processor.Execute(query, p.readQuery())

		if err != nil {
			c.sequence = p.readSequence()
			c.writeError(1064, "Execution error: "+err.Error())
			continue
		}

		c.sequence = p.readSequence()
		if rows != nil {
			c.writeRowset(*rows)
		} else {
			c.writeOkPacket()
		}
	}
}

func (c *Connection) readPacket() (pak Packet, err error) {
	c.resetConnStatus()
	c.resetSequence()

	pak = Packet{}
	err = pak.readBytes(c)

	return pak, err
}

func (c *Connection) writeInitialPacket() {
	p := Packet{}
	p.appendBytes(0, 0, 0, c.resetSequence())
	p.appendBytes(protocolVersion)
	p.appendStringNulByte("DDB ver 0.1")

	// @todo узнать формат
	// thread id OR connection id
	p.appendBytes(10, 0, 0, 0)

	// first 8 bytes of the plugin provided data (scramble)
	p.appendStringNulByte("12345678")

	// server capabilities (two lower bytes)
	p.appendIntTwoBytes(int(clientProtocol41))
	//p.appendBytes(255, 255)

	// @todo узнать список возможных
	// server character set
	p.appendBytes(33)

	// @todo генерировать из констант
	// server status
	p.appendBytes(2, 0)

	// @todo генерировать из констант
	// server1 capabilities (two upper bytes)
	p.appendBytes(255, 193)

	// length of the scramble
	p.appendBytes(21)

	// reserved, always 0
	p.appendBytes(make([]byte, 10)...)

	// rest of the plugin provided data (at least 12 bytes)
	p.appendStringNulByte("123456789012")
	p.appendStringNulByte("mysql_native_password")

	p.calcucateLength()

	c.connection.Write(p.data)
}

func (c *Connection) readAuthPacket() (err error) {
	p := Packet{}

	if err = p.readBytes(c); err != nil {
		log.Println("readAuthPacket error", err)
		return err
	}

	ap := authPacket{
		length:          p.readLength(),
		flags:           p.read4BytesInt(4),
		maxPacketLength: p.read4BytesInt(8),

		collation: p.data[12],
	}

	pos := 13
	ap.filter = make([]byte, 23)
	copy(ap.filter, p.data[pos:pos+23])

	pos = 13 + 23
	ap.user, pos = p.readStringNulByte(pos)
	// some system trash
	// ap.database, pos = p.readStringNulByte(pos + 2)

	return nil
}

func (c *Connection) writeRowset(rowset rowset.Rowset) {
	c.resetSequence()
	p := Packet{}

	c.writeCmdPacket(byte(len(rowset.Cols)))

	// https://github.com/php/php-src/blob/master/ext/mysqlnd/mysqlnd_wireprotocol.c#L1305
	for i := range rowset.Cols {
		col := rowset.Cols[i]
		p.data = []byte{0, 0, 0, c.nextSequence()}
		p.appendStringLengthEncoded("def")
		p.appendStringLengthEncoded("schema")
		p.appendStringLengthEncoded("table")
		p.appendStringLengthEncoded("table-alias")
		p.appendStringLengthEncoded(col.Name)
		p.appendStringLengthEncoded(col.Name)
		p.appendBytes(12) // length of next part
		p.appendBytes(33, 0) // charset
		p.appendBytes(180, 0, 0, 0) // length
		p.appendBytes(254) // type
		p.appendBytes(131, 64) // flags
		p.appendBytes(0) // decimals
		p.appendBytes(0, 0) // filter
		p.calcucateLength()
		c.connection.Write(p.data)
	}

	for i := range rowset.Rows {
		p = Packet{}
		p.data = []byte{0, 0, 0, c.nextSequence()}
		for j := range rowset.Rows[i] {
			p.appendStringLengthEncoded(rowset.Rows[i][j])
		}
		p.calcucateLength()
		c.connection.Write(p.data)
	}

	c.warningsCount = 0
	c.status = c.status | statusLastRowSent
	c.writeEOF()
}

func (c *Connection) writeOkPacket() {
	c.writeCmdPacket(iOK)
}

func (c *Connection) writeEOF() {
	c.writeCmdPacket(iEOF)
}

func (c *Connection) writeCmdPacket(cmd byte) {
	// cmd - iOK | iOEF
	p := Packet{}
	p.appendBytes(0, 0, 0, c.nextSequence(), cmd)
	p.appendIntegerLengthEncoded(uint64(c.affecterRows))
	p.appendIntegerLengthEncoded(uint64(c.lastInsertId))
	p.appendBytes(byte(c.status), byte(c.status>>8))
	p.appendIntegerLengthEncoded(uint64(c.warningsCount))
	p.appendBytes(0)

	p.calcucateLength()
	c.connection.Write(p.data)
}

func (c *Connection) writeError(code int, err string) {
	// @TODO Собрать ошибки mysql и использовать стандартные коды
	// https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html
	p := Packet{}
	p.appendBytes(0, 0, 0, c.nextSequence(), iERR)
	p.appendBytes(byte(code), byte(code>>8))
	p.appendBytes(0x23)
	p.appendBytes([]byte("42000")...)
	p.appendStringNulByte(err)
	p.calcucateLength()
	c.connection.Write(p.data)
}
