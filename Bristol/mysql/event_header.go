package mysql

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type EventHeader struct {
	Timestamp uint32
	EventType EventType
	ServerId  uint32
	EventSize uint32
	LogPos    uint32
	Flags     eventFlag
}

func (header *EventHeader) Read(data []byte) error {
	buf := bytes.NewBuffer(data)
	return binary.Read(buf, binary.LittleEndian, header)
}

func (header *EventHeader) EventName() string {
	switch header.EventType {
	case UNKNOWN_EVENT:
		return "UNKNOWN_EVENT"
	case START_EVENT_V3:
		return "START_EVENT_V3"
	case QUERY_EVENT:
		return "QUERY_EVENT"
	case STOP_EVENT:
		return "STOP_EVENT"
	case ROTATE_EVENT:
		return "ROTATE_EVENT"
	case INTVAR_EVENT:
		return "INTVAR_EVENT"
	case LOAD_EVENT:
		return "LOAD_EVENT"
	case SLAVE_EVENT:
		return "SLAVE_EVENT"
	case CREATE_FILE_EVENT:
		return "CREATE_FILE_EVENT"
	case APPEND_BLOCK_EVENT:
		return "APPEND_BLOCK_EVENT"
	case EXEC_LOAD_EVENT:
		return "EXEC_LOAD_EVENT"
	case DELETE_FILE_EVENT:
		return "DELETE_FILE_EVENT"
	case NEW_LOAD_EVENT:
		return "NEW_LOAD_EVENT"
	case RAND_EVENT:
		return "RAND_EVENT"
	case USER_VAR_EVENT:
		return "USER_VAR_EVENT"
	case FORMAT_DESCRIPTION_EVENT:
		return "FORMAT_DESCRIPTION_EVENT"
	case XID_EVENT:
		return "XID_EVENT"
	case BEGIN_LOAD_QUERY_EVENT:
		return "BEGIN_LOAD_QUERY_EVENT"
	case EXECUTE_LOAD_QUERY_EVENT:
		return "EXECUTE_LOAD_QUERY_EVENT"
	case TABLE_MAP_EVENT:
		return "TABLE_MAP_EVENT"
	case WRITE_ROWS_EVENTv0:
		return "WRITE_ROWS_EVENTv0"
	case UPDATE_ROWS_EVENTv0:
		return "UPDATE_ROWS_EVENTv0"
	case DELETE_ROWS_EVENTv0:
		return "DELETE_ROWS_EVENTv0"
	case WRITE_ROWS_EVENTv1:
		return "WRITE_ROWS_EVENTv1"
	case UPDATE_ROWS_EVENTv1:
		return "UPDATE_ROWS_EVENTv1"
	case DELETE_ROWS_EVENTv1:
		return "DELETE_ROWS_EVENTv1"
	case INCIDENT_EVENT:
		return "INCIDENT_EVENT"
	case HEARTBEAT_EVENT:
		return "HEARTBEAT_EVENT"
	case IGNORABLE_EVENT:
		return "IGNORABLE_EVENT"
	case ROWS_QUERY_EVENT:
		return "ROWS_QUERY_EVENT"
	case WRITE_ROWS_EVENTv2:
		return "WRITE_ROWS_EVENTv2"
	case UPDATE_ROWS_EVENTv2:
		return "UPDATE_ROWS_EVENTv2"
	case DELETE_ROWS_EVENTv2:
		return "DELETE_ROWS_EVENTv2"
	case GTID_EVENT:
		return "GTID_EVENT"
	case ANONYMOUS_GTID_EVENT:
		return "ANONYMOUS_GTID_EVENT"
	case PREVIOUS_GTIDS_EVENT:
		return "PREVIOUS_GTIDS_EVENT"
	}
	return fmt.Sprintf("%d", header.EventType)
}

func (header *EventHeader) FlagNames() (names []string) {
	if header.Flags&LOG_EVENT_BINLOG_IN_USE_F != 0 {
		names = append(names, "LOG_EVENT_BINLOG_IN_USE_F")
	}
	if header.Flags&LOG_EVENT_FORCED_ROTATE_F != 0 {
		names = append(names, "LOG_EVENT_FORCED_ROTATE_F")
	}
	if header.Flags&LOG_EVENT_THREAD_SPECIFIC_F != 0 {
		names = append(names, "LOG_EVENT_THREAD_SPECIFIC_F")
	}
	if header.Flags&LOG_EVENT_SUPPRESS_USE_F != 0 {
		names = append(names, "LOG_EVENT_SUPPRESS_USE_F")
	}
	if header.Flags&LOG_EVENT_UPDATE_TABLE_MAP_VERSION_F != 0 {
		names = append(names, "LOG_EVENT_UPDATE_TABLE_MAP_VERSION_F")
	}
	if header.Flags&LOG_EVENT_ARTIFICIAL_F != 0 {
		names = append(names, "LOG_EVENT_ARTIFICIAL_F")
	}
	if header.Flags&LOG_EVENT_RELAY_LOG_F != 0 {
		names = append(names, "LOG_EVENT_RELAY_LOG_F")
	}
	if header.Flags&LOG_EVENT_IGNORABLE_F != 0 {
		names = append(names, "LOG_EVENT_IGNORABLE_F")
	}
	if header.Flags&LOG_EVENT_NO_FILTER_F != 0 {
		names = append(names, "LOG_EVENT_NO_FILTER_F")
	}
	if header.Flags&LOG_EVENT_MTS_ISOLATE_F != 0 {
		names = append(names, "LOG_EVENT_MTS_ISOLATE_F")
	}
	if header.Flags & ^(LOG_EVENT_MTS_ISOLATE_F<<1-1) != 0 { // unknown flags
		names = append(names, fmt.Sprint(header.Flags & ^(LOG_EVENT_MTS_ISOLATE_F<<1-1)))
	}
	return names
}
