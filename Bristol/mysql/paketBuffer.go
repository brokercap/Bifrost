package mysql

import (
	"bytes"
	"encoding/binary"
)

func init() {}

type paket struct {
	buf     *bytes.Buffer
	buydata []byte
}

func (This *paket) read(size int) []byte {
	if len(This.buydata) == 0 {
		return This.buf.Next(size)
	}
	l := size - len(This.buydata)
	if l == 0 {
		data := This.buydata[0:size]
		This.buydata = make([]byte, 0)
		return data
	}
	if l < 0 {
		data := This.buydata[0:size]
		This.buydata = This.buydata[size:]
		return data
	} else {
		data := This.buydata[0:]
		This.buydata = make([]byte, 0)
		tmp := This.buf.Next(l)
		for _, b := range tmp {
			data = append(data, b)
		}
		return data
	}
}

func (This *paket) readByte() byte {
	if len(This.buydata) > 0 {
		b := This.buydata[0:1]
		This.buydata = This.buydata[1:]
		return b[0]
	} else {
		b, _ := This.buf.ReadByte()
		return b
	}
}

func (This *paket) unread(data []byte) {
	for _, b := range data {
		This.buydata = append(This.buydata, b)
	}
}

func (This *paket) intRead(size int) int {

	var x int
	switch size {
	case 1:
		var a int8
		b_buf := bytes.NewBuffer(This.read(size))
		binary.Read(b_buf, binary.BigEndian, &a)
		x = int(a)
		break
	case 2:
		buf := This.read(size)
		var y int16
		b_buf := bytes.NewBuffer(buf)
		binary.Read(b_buf, binary.BigEndian, &y)
		x = int(y)
		break
	case 3:
		var a, b, c byte
		b_buf := bytes.NewBuffer(This.read(3))
		a, _ = b_buf.ReadByte()
		b, _ = b_buf.ReadByte()
		c, _ = b_buf.ReadByte()
		//log.Println( "read_int24_be a:",a," b:",b," c:",c)
		res := (int(a) << 16) | (int(b) << 8) | int(c)
		//log.Println( "read_int24_be a << 16:",a << 16)
		//log.Println( "read_int24_be b << 8:",b << 8)
		//log.Println( "read_int24_be res:",res)
		if res >= 0x800000 {
			res -= 0x1000000
		}
		x = res
		break
	case 4:
		buf := This.read(size)
		var y int32
		b_buf := bytes.NewBuffer(buf)
		binary.Read(b_buf, binary.BigEndian, &y)
		x = int(y)
		break
	case 5:
		var a int32
		var b byte
		b_buf := bytes.NewBuffer(This.read(4))
		binary.Read(b_buf, binary.BigEndian, &a)
		b_buf = bytes.NewBuffer(This.read(1))
		b, _ = b_buf.ReadByte()
		x = int(b) + (int(a) << 8)
		break
	case 8:
		buf := This.read(size)
		var y int64
		b_buf := bytes.NewBuffer(buf)
		binary.Read(b_buf, binary.BigEndian, &y)
		x = int(y)
		break
	}

	return x
}
