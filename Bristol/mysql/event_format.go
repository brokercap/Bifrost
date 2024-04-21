package mysql

import (
	"bytes"
	"encoding/binary"
)

type FormatDescriptionEvent struct {
	header                 EventHeader
	binlogVersion          uint16
	mysqlServerVersion     string
	createTimestamp        uint32
	eventHeaderLength      uint8
	eventTypeHeaderLengths []uint8
}

func (parser *eventParser) parseFormatDescriptionEvent(buf *bytes.Buffer) (event *FormatDescriptionEvent, err error) {
	event = new(FormatDescriptionEvent)
	err = binary.Read(buf, binary.LittleEndian, &event.header)
	err = binary.Read(buf, binary.LittleEndian, &event.binlogVersion)
	event.mysqlServerVersion = string(buf.Next(50))
	err = binary.Read(buf, binary.LittleEndian, &event.createTimestamp)
	event.eventHeaderLength, err = buf.ReadByte()
	event.eventTypeHeaderLengths = buf.Bytes()
	return
}
