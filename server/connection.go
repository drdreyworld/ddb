package server

import (
	"net"
)

type Connection struct {
	listener   *Listener
	connection net.Conn
	sequence   byte
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

func (c *Connection) Handle() {
	c.writeInitialPacket()

	if err = c.readAuthPacket(); err != nil {
		panic(err)
	}

	c.nextSequence()
	c.writeOkPacket(0, 0, statusInAutocommit, 0)

	for {
		c.resetSequence()
		p := Packet{}

		if err = p.readBytes(c); err != nil {
			c.listener.Println("read bytes error: ", err)
			return
		}

		c.listener.Println("read bytes from client:")
		c.listener.Println(p.data)

		query := p.readQuery()

		c.listener.Println("query:", query)

		switch query {
		case "select @@version_comment limit 1":
			c.sequence = p.readSequence()
			c.writeOkPacket(0, 0, statusInAutocommit, 0)
			break
		case "select 'aaa'":
			c.sequence = p.readSequence()
			c.writeAaaResponse()
			break

		case "select * from users":
			c.sequence = p.readSequence()
			c.writeUsersResponse()
			break
		}
	}
}

func (c *Connection) writeInitialPacket() {
	p := Packet{}
	p.appendBytes(0, 0, 0, c.resetSequence())
	p.appendBytes(protocolVersion)
	p.appendStringNulByte("DDB ver 0.1")

	// @todo узнать формат
	// thread id OR connection id
	p.appendBytes(0, 0, 0, 11)

	// first 8 bytes of the plugin provided data (scramble)
	p.appendStringNulByte("12345678")

	// server capabilities (two lower bytes)
	p.appendIntTwoBytes(int(clientProtocol41))

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
	c.listener.Println("initial packet to client")
	c.listener.Println(p.data)
}

func (c *Connection) readAuthPacket() (err error) {
	p := Packet{}

	if err = p.readBytes(c); err != nil {
		return err
	}

	c.listener.Println("read auth packet from client")
	c.listener.Println(p.data)

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
	ap.database, pos = p.readStringNulByte(pos + 2)

	return nil
}

func (c *Connection) writeAaaResponse() {
	p := Packet{}

	c.resetSequence()

	p.data = []byte{}
	p.appendBytes(1, 0, 0, c.nextSequence(), 1)
	p.appendIntegerLengthEncoded(0)
	p.appendIntegerLengthEncoded(0)
	p.appendBytes(2, 0) // status
	p.appendBytes(0, 0) // warnings
	p.calcucateLength()
	c.connection.Write(p.data)
	c.listener.Println(p.data)

	p.data = []byte{}
	p.appendBytes(25, 0, 0, c.nextSequence(), 3, 100, 101, 102, 0, 0, 0, 3, 97, 97, 97, 0, 12, 33, 0, 9, 0, 0, 0, 253, 1, 0, 31, 0, 0)
	p.calcucateLength()
	c.connection.Write(p.data)
	c.listener.Println(p.data)

	p.data = []byte{}
	p.appendBytes(4, 0, 0, c.nextSequence())
	p.appendStringLengthEncoded("Вова")
	p.appendStringLengthEncoded("Петечкин")
	p.calcucateLength()
	c.connection.Write(p.data)
	c.listener.Println(p.data)

	p.data = []byte{}
	p.appendBytes(4, 0, 0, c.nextSequence())
	p.appendStringLengthEncoded("Саша")
	p.appendStringLengthEncoded("Белый")
	p.calcucateLength()
	c.connection.Write(p.data)
	c.listener.Println(p.data)

	p.data = []byte{}
	p.appendBytes(4, 0, 0, c.nextSequence())
	p.appendStringLengthEncoded("Вася")
	p.appendStringLengthEncoded("Пупкин")
	p.calcucateLength()
	c.connection.Write(p.data)
	c.listener.Println(p.data)

	p.data = []byte{}
	p.appendBytes(5, 0, 0, c.nextSequence(), iEOF, 0, 0, 2, 0) // EOF ?
	p.calcucateLength()
	c.connection.Write(p.data)
	c.listener.Println(p.data)
}

func (c *Connection) writeUsersResponse() {
	p := Packet{}

	rowset := CreateUsersRowset()

	c.resetSequence()

	p.data = []byte{}
	p.appendBytes(1, 0, 0, c.nextSequence(), byte(len(rowset.Cols)))
	p.appendIntegerLengthEncoded(0)
	p.appendIntegerLengthEncoded(0)
	p.appendBytes(2, 0) // status
	p.appendBytes(0, 0) // warnings
	p.calcucateLength()
	c.connection.Write(p.data)
	c.listener.Println(p.data)

	for i := range rowset.Cols {
		col := rowset.Cols[i]
		p.data = []byte{}
		p.appendBytes(25, 0, 0, c.nextSequence())
		p.appendBytes(3, 100, 101, 102)
		p.appendBytes(0, 0, 0)
		//p.appendStringLengthEncoded("schema")
		//p.appendStringLengthEncoded("users")
		//p.appendStringLengthEncoded("alias_for_"+col.Name)
		p.appendStringLengthEncoded(col.Name)
		p.appendBytes(0, 12, 33, 0, 9, 0, 0, 0, 253, 1, 0, 31, 0, 0)
		p.calcucateLength()
		c.connection.Write(p.data)
		c.listener.Println(p.data)
	}

	for i := range rowset.Rows {
		p.data = []byte{}
		p.appendBytes(4, 0, 0, c.nextSequence())
		for j := range rowset.Rows[i].cells {
			p.appendStringLengthEncoded(rowset.Rows[i].cells[j])
		}
		p.calcucateLength()
		c.connection.Write(p.data)
		c.listener.Println(p.data)
	}

	c.writeEOF(0, 0, statusInAutocommit|statusLastRowSent,3)
}

func (c *Connection) writeOkPacket(affectedRows, lastInsertId int, status statusFlag, warnings int) {
	p := Packet{}
	p.appendBytes(0, 0, 0, c.nextSequence(), iOK)
	p.appendIntegerLengthEncoded(uint64(affectedRows))
	p.appendIntegerLengthEncoded(uint64(lastInsertId))
	p.appendBytes(byte(status), byte(status>>8)) // status
	p.appendIntegerLengthEncoded(uint64(warnings))

	p.calcucateLength()
	c.connection.Write(p.data)
	c.listener.Println("write OK packet to client")
	c.listener.Println(p.data)
}

func (c *Connection) writeEOF(affectedRows, lastInsertId int, status statusFlag, warnings int) {
	p := Packet{}
	p.appendBytes(0, 0, 0, c.nextSequence(), iEOF)
	p.appendIntegerLengthEncoded(uint64(affectedRows))
	p.appendIntegerLengthEncoded(uint64(lastInsertId))
	p.appendBytes(byte(status), byte(status>>8)) // status
	p.appendIntegerLengthEncoded(uint64(warnings))

	p.calcucateLength()
	c.connection.Write(p.data)
	c.listener.Println("write EOF packet to client")
	c.listener.Println(p.data)
}

