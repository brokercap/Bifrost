package mysql

import (
	"bytes"
	"encoding/binary"
)

type GenericEvent struct {
	header EventHeader
	data   []byte
}

func parseGenericEvent(buf *bytes.Buffer) (event *GenericEvent, err error) {
	event = new(GenericEvent)
	err = binary.Read(buf, binary.LittleEndian, &event.header)
	event.data = buf.Bytes()
	return
}
