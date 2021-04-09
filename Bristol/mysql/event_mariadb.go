package mysql

import (
	"bytes"
	"encoding/binary"
)

type MariadbGTID struct {
	DomainID       uint32
	ServerID       uint32
	SequenceNumber uint64
}

type MariadbGTIDListEvent struct {
	header EventHeader
	GTIDs  []MariadbGTID
}

func (parser *eventParser) MariadbGTIDListEvent(buf *bytes.Buffer) (event *MariadbGTIDListEvent, err error) {
	event = new(MariadbGTIDListEvent)
	err = binary.Read(buf, binary.LittleEndian, &event.header)
	v := binary.LittleEndian.Uint32(buf.Next(4))
	count := v & uint32((1<<28)-1)

	event.GTIDs = make([]MariadbGTID, count)

	for i := uint32(0); i < count; i++ {
		event.GTIDs[i].DomainID = binary.LittleEndian.Uint32(buf.Next(4))
		event.GTIDs[i].ServerID = binary.LittleEndian.Uint32(buf.Next(4))
		event.GTIDs[i].SequenceNumber = binary.LittleEndian.Uint64(buf.Next(8))
	}

	return
}

type MariadbGTIDEvent struct {
	header   EventHeader
	GTID     MariadbGTID
	Flags    byte
	CommitID uint64
}

func (parser *eventParser) MariadbGTIDEvent(buf *bytes.Buffer) (event *MariadbGTIDEvent, err error) {
	event = new(MariadbGTIDEvent)
	err = binary.Read(buf, binary.LittleEndian, &event.header)
	event.GTID.SequenceNumber = binary.LittleEndian.Uint64(buf.Next(8))
	event.GTID.DomainID = binary.LittleEndian.Uint32(buf.Next(4))
	event.GTID.ServerID = event.header.ServerId
	event.Flags, _ = buf.ReadByte()
	if (event.Flags & BINLOG_MARIADB_FL_GROUP_COMMIT_ID) > 0 {
		event.CommitID = binary.LittleEndian.Uint64(buf.Next(8))
	}
	return
}
