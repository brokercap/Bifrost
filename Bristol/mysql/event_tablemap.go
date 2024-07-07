// documentation:
// https://dev.mysql.com/doc/internals/en/table-map-event.html
package mysql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Bitfield []byte

func (bits Bitfield) isSet(index uint) bool {
	return bits[index/8]&(1<<(index%8)) != 0
}

type ColumnType struct {
	column_type FieldType
	name        string
	unsigned    bool
	max_length  uint16
	length_size uint8
	precision   int
	decimals    int
	size        uint16
	bytes       int
	bits        byte
	fsp         uint8
}

type TableMapEvent struct {
	header         EventHeader
	tableId        uint64
	flags          uint16
	schemaName     string
	tableName      string
	columnTypes    []FieldType
	columnMeta     []uint16
	columnMetaData []*ColumnType
	nullBitmap     Bitfield
}

func (event *TableMapEvent) columnTypeNames() (names []string) {
	names = make([]string, len(event.columnTypes))
	for i, t := range event.columnTypes {
		names[i] = fieldTypeName(t)
	}
	return
}

func (event *TableMapEvent) parseColumnMetadata(data []byte) error {
	pos := 0
	//event.columnMeta = make([]uint16, len(event.columnTypes))
	for i, t := range event.columnMetaData {
		switch t.column_type {
		case FIELD_TYPE_STRING:
			var b, c uint8
			b = uint8(data[pos])
			pos += 1
			c = uint8(data[pos])
			pos += 1
			metadata := (b << 8) + c
			if FieldType(b) == FIELD_TYPE_ENUM || FieldType(b) == FIELD_TYPE_SET {
				event.columnMetaData[i].column_type = FieldType(b)
				event.columnMetaData[i].size = uint16(metadata) & 0x00ff
			} else {
				event.columnMetaData[i].max_length = (((uint16(metadata) >> 4) & 0x300) ^ 0x300) + (uint16(metadata) & 0x00ff)
			}
		case FIELD_TYPE_VARCHAR,
			FIELD_TYPE_VAR_STRING,
			FIELD_TYPE_DECIMAL:
			event.columnMetaData[i].max_length = bytesToUint16(data[pos : pos+2])
			pos += 2

		case FIELD_TYPE_BLOB,
			FIELD_TYPE_GEOMETRY,
			FIELD_TYPE_DOUBLE,
			FIELD_TYPE_FLOAT,
			FIELD_TYPE_TINY_BLOB,
			FIELD_TYPE_MEDIUM_BLOB,
			FIELD_TYPE_LONG_BLOB,
			FIELD_TYPE_JSON:
			event.columnMetaData[i].length_size = uint8(data[pos])
			pos += 1

		case FIELD_TYPE_NEWDECIMAL:
			event.columnMetaData[i].precision = int(data[pos])
			pos += 1
			event.columnMetaData[i].decimals = int(data[pos])
			pos += 1

		case FIELD_TYPE_BIT:
			bits := uint8(data[pos])
			pos += 1
			bytes := uint8(data[pos])
			pos += 1
			event.columnMetaData[i].bits = (bytes * 8) + bits
			event.columnMetaData[i].bytes = int((event.columnMetaData[i].bits + 7) / 8)

		case FIELD_TYPE_TIMESTAMP2, FIELD_TYPE_DATETIME2, FIELD_TYPE_TIME2:
			event.columnMetaData[i].fsp = uint8(data[pos])
			pos += 1

		case
			FIELD_TYPE_DATE,
			FIELD_TYPE_DATETIME,
			FIELD_TYPE_TIMESTAMP,
			FIELD_TYPE_TIME,
			FIELD_TYPE_TINY,
			FIELD_TYPE_SHORT,
			FIELD_TYPE_INT24,
			FIELD_TYPE_LONG,
			FIELD_TYPE_LONGLONG,
			FIELD_TYPE_NULL,
			FIELD_TYPE_YEAR,
			FIELD_TYPE_NEWDATE:
			event.columnMetaData[i].max_length = 0

		default:
			return fmt.Errorf("Unknown FieldType %s", fmt.Sprint(t))
		}
	}
	return nil
}

func (parser *eventParser) parseTableMapEvent(buf *bytes.Buffer) (event *TableMapEvent, err error) {
	var byteLength byte
	var columnCount, variableLength uint64

	event = new(TableMapEvent)
	err = binary.Read(buf, binary.LittleEndian, &event.header)
	headerSize := parser.format.eventTypeHeaderLengths[event.header.EventType-1]
	var tableIdSize int
	if headerSize == 6 {
		tableIdSize = 4
	} else {
		tableIdSize = 6
	}
	event.tableId, err = readFixedLengthInteger(buf, tableIdSize)
	err = binary.Read(buf, binary.LittleEndian, &event.flags)
	byteLength, err = buf.ReadByte()
	event.schemaName = string(buf.Next(int(byteLength)))
	_, err = buf.ReadByte()
	byteLength, err = buf.ReadByte()
	event.tableName = string(buf.Next(int(byteLength)))
	_, err = buf.ReadByte()

	columnCount, _, err = readLengthEncodedInt(buf)

	event.columnTypes = make([]FieldType, columnCount)
	event.columnMetaData = make([]*ColumnType, columnCount)
	columnData := buf.Next(int(columnCount))
	for i, b := range columnData {
		event.columnMetaData[i] = &ColumnType{column_type: FieldType(b)}
		event.columnTypes[i] = FieldType(b)
	}

	variableLength, _, err = readLengthEncodedInt(buf)
	if err = event.parseColumnMetadata(buf.Next(int(variableLength))); err != nil {
		return
	}

	if buf.Len() < int((columnCount+7)/8) {
		err = io.EOF
	}
	event.nullBitmap = Bitfield(buf.Next(int((columnCount + 7) / 8)))
	return
}
