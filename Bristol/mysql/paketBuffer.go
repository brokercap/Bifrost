package mysql

import (
	"bytes"
)

func init() {}

type paket struct {
	buf     *bytes.Buffer
	buydata []byte
}

func (This *paket) read(size int) []byte {
	if len(This.buydata) == size {
		data := This.buydata
		This.buydata = make([]byte, 0)
		return data
	}
	l := size - len(This.buydata)
	if l > 0 {
		This.advance(l)
		return This.read(size)
	} else {
		data := This.buydata[0:size]
		This.buydata = This.buydata[0:size]
		return data
	}
}

func (This *paket) readByte() byte {
	if len(This.buydata) >= 1 {
		b := This.buydata[0:1]
		This.buydata = This.buydata[0:1]
		return b[0]
	} else {
		b, _ := This.buf.ReadByte()
		return b
	}
}

func (This *paket) advance(size int) {
	data := This.buf.Next(size)
	This.unread(data)
}

func (This *paket) unread(data []byte) {
	for _, b := range data {
		This.buydata = append(This.buydata, b)
	}
}
