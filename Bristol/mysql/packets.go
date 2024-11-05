// Packets documentation:
// http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol
package mysql

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

// Read packet to buffer
func (mc *mysqlConn) readPacket() ([]byte, error) {
	pktLen, e := mc.readPktLenAndSeq()
	if e != nil {
		return nil, e
	}

	//超过16MB的断点日志
	// if pktLen >= MAX_PACKET_SIZE {
	// 	log.Printf("more than 16MB pktLen is %v", pktLen)
	// }

	tmpData := make([]byte, pktLen)
	allData, e := mc.readLeftPacket(tmpData, pktLen, 0)
	return allData, e
}

// 读取数据长度与序号
func (mc *mysqlConn) readPktLenAndSeq() (uint64, error) {
	// Packet Length
	pktLen, e := mc.readNumber(3) //读3字节 包长度
	if e != nil {
		return 0, e
	}

	if int(pktLen) == 0 {
		return 0, e
	}

	// Packet Number
	pktSeq, e := mc.readNumber(1) //读1字节
	if e != nil {
		return 0, e
	}

	// Check Packet Sync
	if uint8(pktSeq) != mc.sequence {
		e = errors.New("Commands out of sync; you can't run this command now")
		return 0, e
	}
	mc.sequence++
	return pktLen, nil
}

// 循环读取数据
func (mc *mysqlConn) readLeftPacket(data []byte, pktLen uint64, haveRead uint64) ([]byte, error) {
	var n int
	var e error

	if haveRead != 0 {
		data = append(data, make([]byte, int(pktLen))...) // 超过16MB时即需要迭代自身，每次都要扩大切片的大小(pktLen长度）
	}

	for n < int(pktLen) {
		add, e := mc.bufReader.Read(data[haveRead:]) // 从buffer中读取pktLen长的数据至data切片中
		if e != nil {
			errLog.Print(`packets:58 `, e)
			return nil, e
		}
		n += add
		haveRead = haveRead + uint64(add) // 累加读取的数据长度
	}

	if e != nil || n < int(pktLen) {
		if e == nil {
			e = fmt.Errorf("Length of read data (%d) does not match body length (%d)", n, pktLen)
		}
		errLog.Print(`packets:58 `, e)
		return nil, driver.ErrBadConn
	} else if pktLen >= MAX_PACKET_SIZE {
		pktLen, e := mc.readPktLenAndSeq()
		result, e := mc.readLeftPacket(data, pktLen, haveRead) //pktLen超过16MB则一直迭代自身，直到pktLen小于16MB
		if e != nil {
			errLog.Print("readLeftPacket error occure: ", e)
		}
		return result, e
	}
	//超过16MB的断点日志
	// if haveRead >= MAX_PACKET_SIZE {
	// 	log.Printf("total_pktLen is %v", haveRead)
	// }
	return data, e
}

// Read n bytes long number num
func (mc *mysqlConn) readNumber(nr uint8) (uint64, error) {
	// Read bytes into array
	buf := make([]byte, nr)
	var n, add int
	var e error
	for e == nil && n < int(nr) {
		add, e = mc.bufReader.Read(buf[n:])
		n += add
	}
	if e != nil || n < int(nr) {
		if e == nil {
			e = fmt.Errorf("Length of read data (%d) does not match header length (%d)", n, nr)
		}
		errLog.Print(`packets:78 `, e)
		return 0, driver.ErrBadConn
	}

	// Convert to uint64
	var num uint64 = 0
	for i := uint8(0); i < nr; i++ {
		num |= uint64(buf[i]) << (i * 8)
	}
	return num, e
}

func (mc *mysqlConn) writePacket(data *[]byte) error {
	// Set time BEFORE to avoid possible collisions
	if mc.server.keepalive > 0 {
		mc.lastCmdTime = time.Now()
	}

	// Write packet
	n, e := mc.netConn.Write(*data)
	if e != nil || n != len(*data) {
		if e == nil {
			e = errors.New("Length of send data does not match packet length")
		}
		errLog.Print(`packets:102 `, e)
		return driver.ErrBadConn
	}

	mc.sequence++
	return nil
}

/******************************************************************************
*                             Command Packets                                 *
******************************************************************************/

/*
	Command Packet

Bytes                        Name
-----                        ----
1                            command
n                            arg
*/
func (mc *mysqlConn) writeCommandPacket(command commandType, args ...interface{}) (e error) {
	// Reset Packet Sequence

	mc.sequence = 0

	var arg []byte

	switch command {

	// Commands without args
	case COM_QUIT, COM_PING:
		if len(args) > 0 {
			return fmt.Errorf("Too much arguments (Got: %d Has: 0)", len(args))
		}
		arg = []byte{}

	// Commands with 1 arg unterminated string
	case COM_QUERY, COM_STMT_PREPARE:
		if len(args) != 1 {
			return fmt.Errorf("Invalid arguments count (Got: %d Has: 1)", len(args))
		}
		arg = []byte(args[0].(string))

	// Commands with 1 arg 32 bit uint
	case COM_STMT_CLOSE:
		if len(args) != 1 {
			return fmt.Errorf("Invalid arguments count (Got: %d Has: 1)", len(args))
		}
		arg = uint32ToBytes(args[0].(uint32))

	case COM_BINLOG_DUMP:
		if len(args) != 4 {
			return fmt.Errorf("Invalid arguments count (Got: %d Has: 4)", len(args))
		}
		arg = uint32ToBytes(args[0].(uint32))
		arg = append(arg, uint16ToBytes(args[1].(uint16))...)
		arg = append(arg, uint32ToBytes(args[2].(uint32))...)
		arg = append(arg, []byte(args[3].(string))...)

	case COM_BINLOG_DUMP_GTID:
		if len(args) != 3 {
			return fmt.Errorf("Invalid arguments count (Got: %d Has: 1)", len(args))
		}
		GtidBody := args[0].([]byte)
		/**
		binlog_flags 2
		server_id 4
		binlog_name_info_size 4
		empty binlog name ""
		binlog_pos_info_size 8
		encoded_data_size 4
		*/
		fileNameByte := []byte("")
		arg = append(arg, uint16ToBytes(args[1].(uint16))...)          // 2
		arg = append(arg, uint32ToBytes(args[2].(uint32))...)          // 4
		arg = append(arg, uint32ToBytes(uint32(len(fileNameByte)))...) // 4
		arg = append(arg, fileNameByte...)                             // ""
		arg = append(arg, uint64ToBytes(4)...)                         // 8
		arg = append(arg, uint32ToBytes(uint32(len(GtidBody)))...)     // 4
		arg = append(arg, GtidBody...)                                 // body

	default:
		return fmt.Errorf("Unknown command: %d", command)
	}

	pktLen := 1 + len(arg)
	data := make([]byte, 0, pktLen+4)

	// Add the packet header
	data = append(data, uint24ToBytes(uint32(pktLen))...)
	data = append(data, mc.sequence)

	// Add command byte
	data = append(data, byte(command))

	// Add arg
	data = append(data, arg...)

	// Send CMD packet
	return mc.writePacket(&data)
}

/******************************************************************************
*                              Result Packets                                 *
******************************************************************************/

// Returns error if Packet is not an 'Result OK'-Packet
func (mc *mysqlConn) readResultOK() (e error) {
	data, e := mc.readPacket()
	if e != nil {
		return
	}

	switch data[0] {
	// OK
	case 0:
		return mc.handleOkPacket(data)
	// ERROR
	case 255:
		return mc.handleErrorPacket(data)
	default:
		e = errors.New("Invalid Result Packet-Type")
		return
	}

	return
}

/*
	Error Packet

Bytes                       Name
-----                       ----
1                           field_count, always = 0xff
2                           errno
1                           (sqlstate marker), always '#'
5                           sqlstate (5 characters)
n                           message
*/
func (mc *mysqlConn) handleErrorPacket(data []byte) (e error) {
	if data[0] != 255 {
		e = errors.New("Wrong Packet-Type: Not an Error-Packet")
		return
	}

	pos := 1

	// Error Number [16 bit uint]
	errno := bytesToUint16(data[pos : pos+2])
	pos += 2

	// SQL State [# + 5bytes string]
	//sqlstate := string(data[pos : pos+6])
	pos += 6

	// Error Message [string]
	message := string(data[pos:])

	e = fmt.Errorf("Error %d: %s", errno, message)
	return
}

/*
	Ok Packet

Bytes                       Name
-----                       ----
1   (Length Coded Binary)   field_count, always = 0
1-9 (Length Coded Binary)   affected_rows
1-9 (Length Coded Binary)   insert_id
2                           server_status
2                           warning_count
n   (until end of packet)   message
*/
func (mc *mysqlConn) handleOkPacket(data []byte) (e error) {
	if data[0] != 0 {
		e = errors.New("Wrong Packet-Type: Not an OK-Packet")
		return
	}

	// Position
	pos := 1

	// Affected rows [Length Coded Binary]
	affectedRows, n, e := bytesToLengthCodedBinary(data[pos:])
	if e != nil {
		return
	}
	pos += n

	// Insert id [Length Coded Binary]
	insertID, n, e := bytesToLengthCodedBinary(data[pos:])
	if e != nil {
		return
	}

	// Skip remaining data

	mc.affectedRows = affectedRows
	mc.insertId = insertID

	return
}

/*
	Result Set Header Packet
	Bytes                        Name
	-----                        ----
	1-9   (Length-Coded-Binary)  field_count
	1-9   (Length-Coded-Binary)  extra

The order of packets for a result set is:

	(Result Set Header Packet)  the number of columns
	(Field Packets)             column descriptors
	(EOF Packet)                marker: end of Field Packets
	(Row Data Packets)          row contents
	(EOF Packet)                marker: end of Data Packets
*/
func (mc *mysqlConn) readResultSetHeaderPacket() (fieldCount int, e error) {
	data, e := mc.readPacket()
	if e != nil {
		errLog.Print(`packets:437 `, e)
		e = driver.ErrBadConn
		return
	}

	if data[0] == 255 {
		e = mc.handleErrorPacket(data)
		return
	} else if data[0] == 0 {
		e = mc.handleOkPacket(data)
		return
	}

	num, n, e := bytesToLengthCodedBinary(data)
	if e != nil || (n-len(data)) != 0 {
		e = errors.New("Malformed Packet")
		return
	}

	fieldCount = int(num)
	return
}

// Read Packets as Field Packets until EOF-Packet or an Error appears
func (mc *mysqlConn) readColumns(n int) (columns []mysqlField, e error) {
	var data []byte

	for {
		data, e = mc.readPacket()
		if e != nil {
			return
		}

		// EOF Packet
		if data[0] == 254 && len(data) == 5 {
			if len(columns) != n {
				e = fmt.Errorf("ColumnsCount mismatch n:%d len:%d", n, len(columns))
			}
			return
		}

		var pos, n int
		var name []byte
		//var catalog, database, table, orgTable, name, orgName []byte
		//var defaultVal uint64

		// Catalog
		//catalog, n, _, e = readLengthCodedBinary(data)
		n, e = readAndDropLengthCodedBinary(data)
		if e != nil {
			return
		}
		pos += n

		// Database [len coded string]
		//database, n, _, e = readLengthCodedBinary(data[pos:])
		n, e = readAndDropLengthCodedBinary(data[pos:])
		if e != nil {
			return
		}
		pos += n

		// Table [len coded string]
		//table, n, _, e = readLengthCodedBinary(data[pos:])
		n, e = readAndDropLengthCodedBinary(data[pos:])
		if e != nil {
			return
		}
		pos += n

		// Original table [len coded string]
		//orgTable, n, _, e = readLengthCodedBinary(data[pos:])
		n, e = readAndDropLengthCodedBinary(data[pos:])
		if e != nil {
			return
		}
		pos += n

		// Name [len coded string]
		name, n, _, e = readLengthCodedBinary(data[pos:])
		if e != nil {
			return
		}
		pos += n

		// Original name [len coded string]
		//orgName, n, _, e = readLengthCodedBinary(data[pos:])
		n, e = readAndDropLengthCodedBinary(data[pos:])
		if e != nil {
			return
		}
		pos += n

		// Filler
		pos++

		// Charset [16 bit uint]
		//charsetNumber := bytesToUint16(data[pos : pos+2])
		pos += 2

		// Length [32 bit uint]
		length := bytesToUint32(data[pos : pos+4])
		pos += 4

		// Field type [byte]
		fieldType := FieldType(data[pos])
		pos++

		// Flags [16 bit uint]
		flags := FieldFlag(bytesToUint16(data[pos : pos+2]))
		//pos += 2

		// Decimals [8 bit uint]
		//decimals := data[pos]
		//pos++

		// Default value [len coded binary]
		//if pos < len(data) {
		//	defaultVal, _, e = bytesToLengthCodedBinary(data[pos:])
		//}

		columns = append(columns, mysqlField{name: string(name), fieldType: fieldType, flags: flags, length: length})
	}

	return
}

// Read Packets as Field Packets until EOF-Packet or an Error appears
func (mc *mysqlConn) readRows(columnsCount int) (rows []*[][]byte, e error) {
	var data []byte
	var i, pos, n int
	var isNull bool

	for {
		data, e = mc.readPacket()
		if e != nil {
			return
		}

		// EOF Packet
		if data[0] == 254 && len(data) == 5 {
			return
		}

		// RowSet Packet
		row := make([][]byte, columnsCount)
		pos = 0
		for i = 0; i < columnsCount; i++ {
			// Read bytes and convert to string
			row[i], n, isNull, e = readLengthCodedBinary(data[pos:])
			if e != nil {
				return
			}

			// Append nil if field is NULL
			if isNull {
				row[i] = nil
			}
			pos += n
		}
		rows = append(rows, &row)
	}

	mc.affectedRows = uint64(len(rows))
	return
}

// Reads Packets Packets until EOF-Packet or an Error appears. Returns count of Packets read
func (mc *mysqlConn) readUntilEOF() (count uint64, e error) {
	var data []byte

	for {
		data, e = mc.readPacket()
		if e != nil {
			return
		}

		// EOF Packet
		if data[0] == 254 && len(data) == 5 {
			return
		}

		count++
	}
	return
}

/******************************************************************************
*                           Prepared Statements                               *
******************************************************************************/

/*
	Prepare Result Packets
	Type Of Result Packet       Hexadecimal Value Of First Byte (field_count)
	---------------------       ---------------------------------------------

	Prepare OK Packet           00
	Error Packet                ff

Prepare OK Packet

	Bytes              Name
	-----              ----
	1                  0 - marker for OK packet
	4                  statement_handler_id
	2                  number of columns in result set
	2                  number of parameters in query
	1                  filler (always 0)
	2                  warning count

	It is made up of:

	   a PREPARE_OK packet
	   if "number of parameters" > 0
	       (field packets) as in a Result Set Header Packet
	       (EOF packet)
	   if "number of columns" > 0
	       (field packets) as in a Result Set Header Packet
	       (EOF packet)
*/
func (stmt mysqlStmt) readPrepareResultPacket() (columnCount uint16, e error) {
	data, e := stmt.mc.readPacket()
	if e != nil {
		return
	}

	// Position
	pos := 0

	if data[pos] != 0 {
		e = stmt.mc.handleErrorPacket(data)
		return
	}
	pos++

	stmt.id = bytesToUint32(data[pos : pos+4])
	pos += 4

	// Column count [16 bit uint]
	columnCount = bytesToUint16(data[pos : pos+2])
	pos += 2

	// Param count [16 bit uint]
	stmt.paramCount = int(bytesToUint16(data[pos : pos+2]))
	pos += 2

	// Warning count [16 bit uint]
	// bytesToUint16(data[pos : pos+2])

	return
}

/*
	Command Packet

Bytes                Name
-----                ----
1                    code
4                    statement_id
1                    flags
4                    iteration_count

	if param_count > 0:

(param_count+7)/8    null_bit_map
1                    new_parameter_bound_flag

	if new_params_bound == 1:

n*2                  type of parameters
n                    values for the parameters
*/
func (stmt mysqlStmt) buildExecutePacket(args *[]driver.Value) (e error) {
	argsLen := len(*args)
	if argsLen < stmt.paramCount {
		return fmt.Errorf(
			"Not enough Arguments to call STMT_EXEC (Got: %d Has: %d",
			argsLen,
			stmt.paramCount)
	}

	// Reset packet-sequence
	stmt.mc.sequence = 0
	pktLen := 1 + 4 + 1 + 4 + (stmt.paramCount+7)/8 + 1 + argsLen*2
	paramValues := make([][]byte, 0, argsLen)
	paramTypes := make([]byte, 0, argsLen*2)

	var i, valLen int
	var pv reflect.Value

	var nullMask []byte
	maskLen := (argsLen + 7) / 8
	nullMask = make([]byte, maskLen)
	for i := 0; i < maskLen; i++ {
		nullMask[i] = 0
	}
	for i = 0; i < stmt.paramCount; i++ {
		// build nullBitMap
		if (*args)[i] == nil {
			nullMask[i/8] |= 1 << (uint(i) & 7)
			paramTypes = append(paramTypes, []byte{
				byte(FIELD_TYPE_NULL),
				0x0}...)
			continue
		}

		// cache types and values
		switch (*args)[i].(type) {
		case []byte:
			paramTypes = append(paramTypes, []byte{byte(FIELD_TYPE_STRING), 0x0}...)
			val := (*args)[i].([]byte)
			valLen = len(val)
			lcb := lengthCodedBinaryToBytes(uint64(valLen))
			pktLen += len(lcb) + valLen
			paramValues = append(paramValues, lcb)
			paramValues = append(paramValues, val)
			continue

		case time.Time:
			// Format to string for time+date Fields
			// Data is packed in case reflect.String below
			(*args)[i] = (*args)[i].(time.Time).Format(TIME_FORMAT)
		}

		pv = reflect.ValueOf((*args)[i])
		switch pv.Kind() {
		case reflect.Int64:
			paramTypes = append(paramTypes, []byte{byte(FIELD_TYPE_LONGLONG), 0x0}...)
			val := int64ToBytes(pv.Int())
			pktLen += len(val)
			paramValues = append(paramValues, val)
			continue

		case reflect.Float64:
			paramTypes = append(paramTypes, []byte{byte(FIELD_TYPE_DOUBLE), 0x0}...)
			val := float64ToBytes(pv.Float())
			pktLen += len(val)
			paramValues = append(paramValues, val)
			continue

		case reflect.Bool:
			paramTypes = append(paramTypes, []byte{byte(FIELD_TYPE_TINY), 0x0}...)
			val := pv.Bool()
			pktLen++
			if val {
				paramValues = append(paramValues, []byte{byte(1)})
			} else {
				paramValues = append(paramValues, []byte{byte(0)})
			}
			continue

		case reflect.String:
			paramTypes = append(paramTypes, []byte{byte(FIELD_TYPE_STRING), 0x0}...)
			val := []byte(pv.String())
			valLen = len(val)
			lcb := lengthCodedBinaryToBytes(uint64(valLen))
			pktLen += valLen + len(lcb)
			paramValues = append(paramValues, lcb)
			paramValues = append(paramValues, val)
			continue

		default:
			return fmt.Errorf("Invalid Value: %s", pv.Kind().String())
		}
	}

	data := make([]byte, 0, pktLen+4)

	// Add the packet header
	data = append(data, uint24ToBytes(uint32(pktLen))...)
	data = append(data, stmt.mc.sequence)

	// code [1 byte]
	data = append(data, byte(COM_STMT_EXECUTE))

	// statement_id [4 bytes]
	data = append(data, uint32ToBytes(stmt.id)...)

	// flags (0: CURSOR_TYPE_NO_CURSOR) [1 byte]
	data = append(data, byte(0))

	// iteration_count [4 bytes]
	data = append(data, uint32ToBytes(1)...)

	if stmt.paramCount > 0 {
		data = append(data, nullMask...)
	}

	// newParameterBoundFlag 1 [1 byte]
	data = append(data, byte(1))

	// type of parameters [n*2 byte]
	data = append(data, paramTypes...)

	// values for the parameters [n byte]
	for _, paramValue := range paramValues {
		data = append(data, paramValue...)
	}

	return stmt.mc.writePacket(&data)
}

func (mc *mysqlConn) readBinaryRows(rc *rowsContent) (e error) {
	var data, nullBitMap []byte
	var i, pos, n int
	var unsigned, isNull bool
	columnsCount := len(rc.columns)

	for {
		data, e = mc.readPacket()
		if e != nil {
			return
		}

		pos = 0

		// EOF Packet
		if data[pos] == 254 && len(data) == 5 {
			return
		}

		pos++

		// BinaryRowSet Packet
		//row := make([][]byte, columnsCount)
		row := make([]driver.Value, columnsCount)

		nullBitMap = data[pos : pos+(columnsCount+7+2)/8]
		pos += (columnsCount + 7 + 2) / 8

		for i = 0; i < columnsCount; i++ {
			// Field is NULL
			if (nullBitMap[(i+2)/8] >> uint((i+2)%8) & 1) == 1 {
				row[i] = nil
				continue
			}

			unsigned = rc.columns[i].flags&FLAG_UNSIGNED != 0

			// Convert to byte-coded string
			switch rc.columns[i].fieldType {
			case FIELD_TYPE_NULL:
				row[i] = nil
				break

			// Numeric Typs
			case FIELD_TYPE_TINY:
				if unsigned {
					//row[i] = uintToByteStr(uint64(byteToUint8(data[pos])))
					row[i] = byteToUint8(data[pos])
				} else {
					//row[i] = intToByteStr(int64(int8(byteToUint8(data[pos]))))
					b := int8(byteToUint8(data[pos]))
					//length == 1 是 tinyint(1)  bool值
					if rc.columns[i].length == 1 {
						switch b {
						case 1:
							row[i] = true
						case 0:
							row[i] = false
						default:
							row[i] = b
						}
					} else {
						row[i] = b
					}
				}
				pos++
				break

			case FIELD_TYPE_SHORT:
				if unsigned {
					//row[i] = uintToByteStr(uint64(bytesToUint16(data[pos : pos+2])))
					row[i] = bytesToUint16(data[pos : pos+2])
				} else {
					//row[i] = intToByteStr(int64(int16(bytesToUint16(data[pos : pos+2]))))
					row[i] = int16(bytesToUint16(data[pos : pos+2]))
				}
				pos += 2
				break

			case FIELD_TYPE_YEAR:
				row[i] = strconv.Itoa(int(bytesToUint16(data[pos : pos+2])))
				pos += 2
				break

			case FIELD_TYPE_INT24, FIELD_TYPE_LONG:
				if unsigned {
					row[i] = bytesToUint32(data[pos : pos+4])
				} else {
					row[i] = int32(bytesToUint32(data[pos : pos+4]))
				}
				pos += 4
				break

			case FIELD_TYPE_LONGLONG:
				if unsigned {
					row[i] = bytesToUint64(data[pos : pos+8])
				} else {
					row[i] = int64(bytesToUint64(data[pos : pos+8]))
				}
				pos += 8
				break

			case FIELD_TYPE_FLOAT:
				row[i] = bytesToFloat32(data[pos : pos+4])
				pos += 4
				break

			case FIELD_TYPE_DOUBLE:
				row[i] = bytesToFloat64(data[pos : pos+8])
				pos += 8
				break

			case FIELD_TYPE_DECIMAL, FIELD_TYPE_NEWDECIMAL:
				var b []byte
				b, n, isNull, e = readLengthCodedBinary(data[pos:])
				if e != nil {
					return
				}
				if isNull && rc.columns[i].flags&FLAG_NOT_NULL == 0 {
					row[i] = nil
				} else {
					row[i] = string(b)
				}
				pos += n
				break

			// Length coded Binary Strings
			case FIELD_TYPE_VARCHAR, FIELD_TYPE_ENUM,
				FIELD_TYPE_SET, FIELD_TYPE_TINY_BLOB, FIELD_TYPE_MEDIUM_BLOB,
				FIELD_TYPE_LONG_BLOB, FIELD_TYPE_BLOB, FIELD_TYPE_VAR_STRING,
				FIELD_TYPE_STRING, FIELD_TYPE_GEOMETRY, FIELD_TYPE_JSON:
				var b []byte
				b, n, isNull, e = readLengthCodedBinary(data[pos:])
				if e != nil {
					return
				}

				if isNull && rc.columns[i].flags&FLAG_NOT_NULL == 0 {
					row[i] = nil
				} else {
					row[i] = string(b)
				}
				pos += n
				break
			case FIELD_TYPE_BIT:
				var bb []byte
				bb, n, isNull, e = readLengthCodedBinary(data[pos:])
				if e != nil {
					return
				}
				if isNull && rc.columns[i].flags&FLAG_NOT_NULL == 0 {
					row[i] = nil
					break
				}
				row[i], _ = bitBytes2Int64(bb)
				pos += n
				break

				// Date YYYY-MM-DD
			case FIELD_TYPE_DATE, FIELD_TYPE_NEWDATE:
				var num uint64
				num, n, e = bytesToLengthCodedBinary(data[pos:])
				if e != nil {
					return
				}
				pos += n

				if num == 0 {
					row[i] = "0000-00-00"
				} else {
					row[i] = fmt.Sprintf("%04d-%02d-%02d",
						bytesToUint16(data[pos:pos+2]),
						data[pos+2],
						data[pos+3])
				}
				pos += int(num)

			// Time HH:MM:SS
			case FIELD_TYPE_TIME:
				var num uint64
				num, n, e = bytesToLengthCodedBinary(data[pos:])
				if e != nil {
					return
				}
				switch num {
				case 0:
					row[i] = "00:00:00"
				case 8:
					row[i] = fmt.Sprintf("%02d:%02d:%02d",
						data[pos+6],
						data[pos+7],
						data[pos+8])
				case 12:
					row[i] = fmt.Sprintf(
						"%02d:%02d:%02d.%06d",
						data[pos+6],
						data[pos+7],
						data[pos+8],
						bytesToUint24(data[pos+9:pos+12]))
				default:
					return fmt.Errorf("Invalid time-packet length %d", num)
				}
				pos += n + int(num)
				break

			// Timestamp YYYY-MM-DD HH:MM:SS
			case FIELD_TYPE_TIMESTAMP, FIELD_TYPE_DATETIME:
				var num uint64
				num, n, e = bytesToLengthCodedBinary(data[pos:])
				if e != nil {
					return
				}
				pos += n

				switch num {
				case 0:
					row[i] = "0000-00-00 00:00:00"
				case 4:
					row[i] = fmt.Sprintf("%04d-%02d-%02d 00:00:00",
						bytesToUint16(data[pos:pos+2]),
						data[pos+2],
						data[pos+3])
				case 7:
					row[i] = fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
						bytesToUint16(data[pos:pos+2]),
						data[pos+2],
						data[pos+3],
						data[pos+4],
						data[pos+5],
						data[pos+6])
				case 11:
					row[i] = fmt.Sprintf(
						"%04d-%02d-%02d %02d:%02d:%02d.%06d",
						bytesToUint16(data[pos:pos+2]),
						data[pos+2],
						data[pos+3],
						data[pos+4],
						data[pos+5],
						data[pos+6],
						bytesToUint32(data[pos+7:pos+11]))
				default:
					return fmt.Errorf("Invalid datetime-packet length %d", num)
				}
				pos += int(num)
				break
			// Please report if this happens!
			default:
				return fmt.Errorf("Unknown FieldType %d", rc.columns[i].fieldType)
			}
		}
		rc.rows = append(rc.rows, row)
	}

	mc.affectedRows = uint64(len(rc.rows))
	return
}

func (mc *mysqlConn) readStringRows(rc *rowsContent) (e error) {
	var data []byte
	var i, pos, n int
	var unsigned, isNull bool
	columnsCount := len(rc.columns)
	rc.rows = make([][]driver.Value, 0)
	for {
		data, e = mc.readPacket()
		if e != nil {
			return
		}
		pos = 0
		// EOF Packet
		if data[pos] == 254 && len(data) == 5 {
			return
		}
		row := make([]driver.Value, columnsCount)
		for i = 0; i < columnsCount; i++ {
			var b []byte
			b, n, isNull, e = readLengthCodedBinary(data[pos:])
			pos += n
			if isNull && rc.columns[i].flags&FLAG_NOT_NULL == 0 {
				row[i] = nil
				continue
			}
			dataStr := string(b)
			// Convert to byte-coded string
			switch rc.columns[i].fieldType {
			case FIELD_TYPE_NULL:
				row[i] = nil
				break

			// Numeric Typs
			case FIELD_TYPE_TINY:
				intN, _ := strconv.Atoi(dataStr)
				unsigned = rc.columns[i].flags&FLAG_UNSIGNED != 0
				if unsigned {
					row[i] = uint8(intN)
				} else {
					if rc.columns[i].length == 1 {
						switch intN {
						case 1:
							row[i] = true
						case 0:
							row[i] = false
						default:
							row[i] = int8(intN)
						}
					} else {
						row[i] = int8(intN)
					}
				}
				break
			case FIELD_TYPE_SHORT:
				intN, _ := strconv.Atoi(dataStr)
				unsigned = rc.columns[i].flags&FLAG_UNSIGNED != 0
				if unsigned {
					row[i] = uint16(intN)
				} else {
					row[i] = int16(intN)
				}
				break
			case FIELD_TYPE_INT24, FIELD_TYPE_LONG:
				intN, _ := strconv.Atoi(dataStr)
				unsigned = rc.columns[i].flags&FLAG_UNSIGNED != 0
				if unsigned {
					row[i] = uint32(intN)
				} else {
					row[i] = int32(intN)
				}
				break
			case FIELD_TYPE_LONGLONG:
				unsigned = rc.columns[i].flags&FLAG_UNSIGNED != 0
				if unsigned {
					row[i], _ = strconv.ParseUint(dataStr, 10, 64)
				} else {
					row[i], _ = strconv.ParseInt(dataStr, 10, 64)
				}
				break
			case FIELD_TYPE_FLOAT:
				floatN, _ := strconv.ParseFloat(dataStr, 32)
				row[i] = float32(floatN)
				break
			case FIELD_TYPE_DOUBLE:
				row[i], _ = strconv.ParseFloat(dataStr, 64)
				break
			case FIELD_TYPE_BIT:
				row[i], _ = bitBytes2Int64([]byte(dataStr))
				break
			default:
				row[i] = string(b)
				break
			}
		}
		rc.rows = append(rc.rows, row)
	}

	mc.affectedRows = uint64(len(rc.rows))
	return
}
