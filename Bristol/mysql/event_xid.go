// documentation:
// https://dev.mysql.com/doc/internals/en/xid-event.html
package mysql

import (
	"bytes"
	"encoding/binary"
)

type XIdEvent struct {
	header EventHeader
	xid    int64
}

func (parser *eventParser) parseXidEvent(buf *bytes.Buffer) (event *XIdEvent, err error) {
	event = new(XIdEvent)
	err = binary.Read(buf, binary.LittleEndian, &event.header)
	err = binary.Read(buf, binary.LittleEndian, &event.xid)
	return
}
