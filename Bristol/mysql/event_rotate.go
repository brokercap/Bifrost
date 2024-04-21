package mysql

import (
	"bytes"
	"encoding/binary"
)

type RotateEvent struct {
	header   EventHeader
	position uint64
	filename string
}

func (parser *eventParser) parseRotateEvent(buf *bytes.Buffer) (event *RotateEvent, err error) {
	event = new(RotateEvent)
	err = binary.Read(buf, binary.LittleEndian, &event.header)
	err = binary.Read(buf, binary.LittleEndian, &event.position)
	event.filename = buf.String()
	return
}
