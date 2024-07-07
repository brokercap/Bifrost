// documentation:
// https://dev.mysql.com/doc/internals/en/rows-query-event.html
package mysql

import (
	"bytes"
	"encoding/binary"
)

type QueryEvent struct {
	header        EventHeader
	slaveProxyId  uint32
	executionTime uint32
	errorCode     uint16
	schema        string
	statusVars    string
	query         string
}

func (parser *eventParser) parseQueryEvent(buf *bytes.Buffer) (event *QueryEvent, err error) {
	var schemaLength byte
	var statusVarsLength uint16

	event = new(QueryEvent)
	err = binary.Read(buf, binary.LittleEndian, &event.header)
	err = binary.Read(buf, binary.LittleEndian, &event.slaveProxyId)
	err = binary.Read(buf, binary.LittleEndian, &event.executionTime)
	err = binary.Read(buf, binary.LittleEndian, &schemaLength)
	err = binary.Read(buf, binary.LittleEndian, &event.errorCode)
	err = binary.Read(buf, binary.LittleEndian, &statusVarsLength)
	event.statusVars = string(buf.Next(int(statusVarsLength)))
	event.schema = string(buf.Next(int(schemaLength)))
	_, err = buf.ReadByte()
	event.query = buf.String()
	return
}
