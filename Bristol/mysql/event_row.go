// documentation:
// https://dev.mysql.com/doc/internals/en/rows-event.html
// https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType
package mysql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"
)

type RowsEvent struct {
	header                EventHeader
	tableId               uint64
	flags                 uint16
	columnsPresentBitmap1 Bitfield
	columnsPresentBitmap2 Bitfield
	rows                  []map[string]interface{}
}

func (parser *eventParser) parseRowsEvent(buf *bytes.Buffer) (event *RowsEvent, err error) {
	var columnCount uint64

	event = new(RowsEvent)
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
	switch event.header.EventType {
	case UPDATE_ROWS_EVENTv2, WRITE_ROWS_EVENTv2, DELETE_ROWS_EVENTv2:
		//err = binary.Read(buf, binary.LittleEndian, &event.flags)
		extraDataLength, _ := readFixedLengthInteger(buf, 2)
		buf.Next(int(extraDataLength) - 2)
		break
	}

	columnCount, _, err = readLengthEncodedInt(buf)

	event.columnsPresentBitmap1 = Bitfield(buf.Next(int((columnCount + 7) / 8)))
	switch event.header.EventType {
	case UPDATE_ROWS_EVENTv1, UPDATE_ROWS_EVENTv2:
		event.columnsPresentBitmap2 = Bitfield(buf.Next(int((columnCount + 7) / 8)))
	}
	//假如 map event 已经过滤了当前库，则直接不再解析
	if parser.filterNextRowEvent == true {
		return
	}
	for buf.Len() > 0 {
		var row map[string]interface{}
		row, err = parser.parseEventRow(buf, parser.tableMap[event.tableId], parser.tableSchemaMap[event.tableId].ColumnSchemaTypeList)
		if err != nil {
			log.Println("event row parser err:", err)
			return
		}
		event.rows = append(event.rows, row)
	}

	return
}

func (parser *eventParser) parseEventRow(buf *bytes.Buffer, tableMap *TableMapEvent, tableSchemaMap []*ColumnInfo) (row map[string]interface{}, e error) {
	columnsCount := len(tableMap.columnTypes)
	row = make(map[string]interface{})
	bitfieldSize := (columnsCount + 7) / 8
	nullBitMap := Bitfield(buf.Next(bitfieldSize))
	if columnsCount > len(tableSchemaMap) {
		log.Println("parseEventRow len(tableSchemaMap)=", len(tableSchemaMap), " < ", "columnsCount:", columnsCount, " tableMap:", *tableMap)
	}
	for i := 0; i < columnsCount; i++ {
		column_name := tableSchemaMap[i].COLUMN_NAME
		//log.Println("column_name:",column_name,tableSchemaMap[i].DATA_TYPE)
		if nullBitMap.isSet(uint(i)) {
			row[column_name] = nil
			continue
		}
		switch tableMap.columnMetaData[i].column_type {
		case FIELD_TYPE_NULL:
			row[column_name] = nil
			break

		case FIELD_TYPE_TINY:
			var b byte
			b, e = buf.ReadByte()
			if tableSchemaMap[i].IsBool == true {
				switch int(b) {
				case 1:
					row[column_name] = true
				case 0:
					row[column_name] = false
				default:
					if tableSchemaMap[i].Unsigned == true {
						row[column_name] = uint8(b)
					} else {
						row[column_name] = int8(b)
					}
				}
			} else {
				if tableSchemaMap[i].Unsigned == true {
					row[column_name] = uint8(b)
				} else {
					row[column_name] = int8(b)
				}
			}
			break

		case FIELD_TYPE_SHORT:
			if tableSchemaMap[i].Unsigned {
				var short uint16
				e = binary.Read(buf, binary.LittleEndian, &short)
				row[column_name] = short
			} else {
				var short int16
				e = binary.Read(buf, binary.LittleEndian, &short)
				row[column_name] = short
			}
			break

		case FIELD_TYPE_YEAR:
			var b byte
			b, e = buf.ReadByte()
			if e == nil && b != 0 {
				//time.Date(int(b)+1900, time.January, 0, 0, 0, 0, 0, time.UTC)
				row[column_name] = strconv.Itoa(int(b) + 1900)
			}
			break

		case FIELD_TYPE_INT24:
			if tableSchemaMap[i].Unsigned {
				var bint uint64
				bint, e = readFixedLengthInteger(buf, 3)
				row[column_name] = uint32(bint)
			} else {
				var a, b, c uint8

				var tmp byte
				tmp, e = buf.ReadByte()
				a = uint8(tmp)
				tmp, e = buf.ReadByte()
				b = uint8(tmp)
				tmp, e = buf.ReadByte()
				c = uint8(tmp)
				res := int32(a) | (int32(b) << 8) | (int32(c) << 16)
				if res >= 0x800000 {
					res -= 0x1000000
				}
				row[column_name] = res
			}
			break

		case FIELD_TYPE_LONG:
			if tableSchemaMap[i].Unsigned {
				var long uint32
				e = binary.Read(buf, binary.LittleEndian, &long)
				row[column_name] = long
			} else {
				var long int32
				e = binary.Read(buf, binary.LittleEndian, &long)
				row[column_name] = long
			}
			break

		case FIELD_TYPE_LONGLONG:
			if tableSchemaMap[i].Unsigned {
				var longlong uint64
				e = binary.Read(buf, binary.LittleEndian, &longlong)
				row[column_name] = longlong
			} else {
				var longlong int64
				e = binary.Read(buf, binary.LittleEndian, &longlong)
				row[column_name] = longlong
			}
			break

		case FIELD_TYPE_FLOAT:
			var float float32
			e = binary.Read(buf, binary.LittleEndian, &float)
			row[column_name] = float
			break

		case FIELD_TYPE_DOUBLE:
			var double float64
			e = binary.Read(buf, binary.LittleEndian, &double)
			row[column_name] = double
			break

		case FIELD_TYPE_DECIMAL:
			return nil, fmt.Errorf("parseEventRow unimplemented for field type %s", fieldTypeName(tableMap.columnTypes[i]))

		case FIELD_TYPE_NEWDECIMAL:
			digits_per_integer := 9
			compressed_bytes := [10]int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}
			integral := (tableMap.columnMetaData[i].precision - tableMap.columnMetaData[i].decimals)
			uncomp_integral := int(int(integral) / digits_per_integer)
			uncomp_fractional := int(int(tableMap.columnMetaData[i].decimals) / digits_per_integer)
			comp_integral := integral - (uncomp_integral * digits_per_integer)
			comp_fractional := tableMap.columnMetaData[i].decimals - (uncomp_fractional * digits_per_integer)

			/*
				log.Println( "column.precision",tableMap.columnMetaData[i].precision)
				log.Println( "column.decimals",tableMap.columnMetaData[i].decimals)
				log.Println( "uncomp_integral",uncomp_integral)
				log.Println( "uncomp_fractional",uncomp_fractional)
				log.Println( "comp_integral",comp_integral)
				log.Println( "comp_fractional",comp_fractional)
			*/

			var value int
			var res string
			var mask int
			var size int
			size = compressed_bytes[comp_integral]

			bufPaket := &paket{
				buf:     buf,
				buydata: make([]byte, 0),
			}
			b := bufPaket.readByte()
			//log.Println("value:",b)
			if int(b)&128 != 0 {
				res = ""
				mask = 0
			} else {
				mask = -1
				res = "-"
			}

			var tmp *bytes.Buffer = new(bytes.Buffer)
			//Println("value ^ 0x80:",uint8(b) ^ 128)
			binary.Write(tmp, binary.LittleEndian, uint8(b)^128)
			bufPaket.unread(tmp.Bytes())
			//log.Println("first size",size)
			res0 := ""

			if size > 0 {
				v1 := bufPaket.intRead(size)
				//log.Println( "first size d value::",v1)
				value = int(v1) ^ mask
				res0 += strconv.Itoa(value)
			}
			//log.Println( "first res0:",res0)
			for i := 0; i < uncomp_integral; i++ {
				//log.Println( "uncomp_integral ssssssssss:",i)
				s := bufPaket.read(4)
				//log.Println("s:",s,"slen:",len(s))
				b_buf := bytes.NewBuffer(s)
				var x int32
				e = binary.Read(b_buf, binary.BigEndian, &x)
				//log.Println("x:",x)
				value = int(x) ^ mask
				res0 += fmt.Sprintf("%09d", value)
			}
			//log.Println("first res",res)

			res0 = strings.TrimLeft(res0, "0")
			if res0 == "" {
				res += "0."
			} else {
				res += res0 + "."
			}

			for i := 0; i < uncomp_fractional; i++ {
				b_buf := bytes.NewBuffer(bufPaket.read(4))
				var x int32
				e = binary.Read(b_buf, binary.BigEndian, &x)
				value = int(x) ^ mask
				res += fmt.Sprintf("%09d", value)
			}
			//log.Println( "sec res",res)
			size = compressed_bytes[comp_fractional]
			//log.Println("sec size",size)
			if size > 0 {
				ss := bufPaket.intRead(size)
				//log.Println( "sec size int:",ss)
				value = ss ^ mask
				res += fmt.Sprintf("%0*d", comp_fractional, value)
			}
			row[column_name] = res
			//log.Println("column_name:",column_name,"=",row[column_name])
			break

		case FIELD_TYPE_VARCHAR:
			max_length := tableMap.columnMetaData[i].max_length
			var length int
			if max_length > 255 {
				var short uint16
				e = binary.Read(buf, binary.LittleEndian, &short)
				length = int(short)
			} else {
				var b byte
				b, e = buf.ReadByte()
				length = int(b)
			}

			if buf.Len() < length {
				e = io.EOF
				/*
					log.Println(*tableSchemaMap[i])
					log.Println("schemaName:",tableMap.schemaName)
					log.Println("tableName:",tableMap.tableName)
					log.Println("tableId:",tableMap.tableId)
					log.Println("column length:",len(tableMap.columnMetaData))
					log.Println("tableMap.columnTypeNames():",tableMap.columnTypeNames())

					log.Println("name",tableMap.columnMetaData[i].name)
					log.Println("column_type",tableMap.columnMetaData[i].column_type)
					log.Println("unsigned",tableMap.columnMetaData[i].unsigned)
					log.Println("size",tableMap.columnMetaData[i].size)
					log.Println("bits",tableMap.columnMetaData[i].bits)
					log.Println("bytes",tableMap.columnMetaData[i].bytes)
					log.Println("decimals",tableMap.columnMetaData[i].decimals)
					log.Println("fsp",tableMap.columnMetaData[i].fsp)
					log.Println("length_size",tableMap.columnMetaData[i].length_size)
					log.Println("max_length",tableMap.columnMetaData[i].max_length)
					log.Println("precision",tableMap.columnMetaData[i].precision)
					for k,v := range row{
						log.Println("key:",k,"val:",v)
					}
					log.Fatal("FIELD_TYPE_VARCHAR buf len err:",buf.Len(),"<",length," max_length:",max_length," column_name:",column_name)
				*/
			}
			row[column_name] = string(buf.Next(length))
			break

		case FIELD_TYPE_STRING:
			var length int
			if tableSchemaMap[i].CHARACTER_OCTET_LENGTH > 255 {
				var short uint16
				e = binary.Read(buf, binary.LittleEndian, &short)
				length = int(short)
			} else {
				var b byte
				b, e = buf.ReadByte()
				length = int(b)
			}
			//row[column_name] = string(buf.Next(length+1))
			/*
				log.Println("======================")
				log.println("column_name:", column_name," length:",length)
				//log.Println("name:",tableMap.columnMetaData[i].name)
				log.Println("size:",tableMap.columnMetaData[i].size)
				log.Println("precision:",tableMap.columnMetaData[i].precision)
				log.Println("max_length:",tableMap.columnMetaData[i].max_length)
				log.Println("length_size:",tableMap.columnMetaData[i].length_size)
				log.Println("fsp:",tableMap.columnMetaData[i].fsp)
				log.Println("decimals:",tableMap.columnMetaData[i].decimals)
				log.Println("unsigned:",tableMap.columnMetaData[i].unsigned)
				log.Println("column_type:",tableMap.columnMetaData[i].column_type)
				log.Println("tableMap.columnMetaData[i]:",tableMap.columnMetaData[i])
			*/
			row[column_name] = string(buf.Next(length))
			//log.Println("column_name: ",column_name," == ",row[column_name])

			break

		case FIELD_TYPE_ENUM:
			size := tableMap.columnMetaData[i].size
			var index int
			if size == 1 {
				var b byte
				b, _ = buf.ReadByte()
				index = int(b)
			} else {
				index = int(bytesToUint16(buf.Next(int(size))))
			}
			if index < 1 || len(tableSchemaMap[i].EnumValues) < index {
				row[column_name] = nil
			} else {
				row[column_name] = tableSchemaMap[i].EnumValues[index-1]
			}
			break

		case FIELD_TYPE_SET:
			size := tableMap.columnMetaData[i].size
			var index int
			switch size {
			case 0:
				row[column_name] = nil
				break
			case 1:
				var b byte
				b, _ = buf.ReadByte()
				index = int(b)
			case 2:
				index = int(bytesToUint16(buf.Next(int(size))))
			case 3:
				index = int(bytesToUint24(buf.Next(int(size))))
			case 4:
				index = int(bytesToUint32(buf.Next(int(size))))
			default:
				index = 0
			}

			//result := make(map[string]int, 0)
			result := make([]string, 0)

			for i, val := range tableSchemaMap[i].SetValues {
				s := index & mathPowerInt(2, i)
				if s > 0 {
					result = append(result, val)
					//result[val] = 1
				}
			}
			/*
				f := make([]string, 0)
				for key, _ := range result {
					f = append(f, key)
				}
			*/
			row[column_name] = result
			break

		case FIELD_TYPE_BLOB, FIELD_TYPE_TINY_BLOB, FIELD_TYPE_MEDIUM_BLOB,
			FIELD_TYPE_LONG_BLOB, FIELD_TYPE_VAR_STRING:
			var length uint64
			length, e = readFixedLengthInteger(buf, int(tableMap.columnMetaData[i].length_size))
			row[column_name] = string(buf.Next(int(length)))
			break
		case FIELD_TYPE_BIT:
			var resp string = ""
			for k := 0; k < tableMap.columnMetaData[i].bytes; k++ {
				//var current_byte = ""
				var current_byte []string
				var b byte
				var end byte
				b, e = buf.ReadByte()
				var data int
				data = int(b)
				if k == 0 {
					if tableMap.columnMetaData[i].bytes == 1 {
						end = tableMap.columnMetaData[i].bits
					} else {
						end = tableMap.columnMetaData[i].bits % 8
						if end == 0 {
							end = 8
						}
					}
				} else {
					end = 8
				}
				var bit uint
				for bit = 0; bit < uint(end); bit++ {
					tmp := 1 << bit
					if (data & tmp) > 0 {
						current_byte = append(current_byte, "1")
					} else {
						current_byte = append(current_byte, "0")
					}
				}
				for k := len(current_byte); k > 0; k-- {
					resp += current_byte[k-1]
				}
			}
			bitInt, _ := strconv.ParseInt(resp, 2, 10)
			row[column_name] = bitInt
			break

		case
			FIELD_TYPE_GEOMETRY:
			return nil, fmt.Errorf("parseEventRow unimplemented for field type %s", fieldTypeName(tableMap.columnTypes[i]))

		case FIELD_TYPE_DATE, FIELD_TYPE_NEWDATE:
			var data []byte
			data = buf.Next(3)
			timeInt := int(int(data[0]) + (int(data[1]) << 8) + (int(data[2]) << 16))
			if timeInt == 0 {
				if tableSchemaMap[i].COLUMN_DEFAULT != "" {
					row[column_name] = tableSchemaMap[i].COLUMN_DEFAULT
				} else {
					row[column_name] = nil
				}
			} else {
				year := (timeInt & (((1 << 15) - 1) << 9)) >> 9
				month := (timeInt & (((1 << 4) - 1) << 5)) >> 5
				day := (timeInt & ((1 << 5) - 1))
				t := fmt.Sprintf("%4d-%02d-%02d", year, month, day)
				row[column_name] = t
			}
			break

		case FIELD_TYPE_TIME:
			var data []byte
			data = buf.Next(3)
			timeInt := int(int(data[0]) + (int(data[1]) << 8) + (int(data[2]) << 16))
			if timeInt == 0 {
				if tableSchemaMap[i].COLUMN_DEFAULT != "" {
					row[column_name] = tableSchemaMap[i].COLUMN_DEFAULT
				} else {
					row[column_name] = nil
				}
			} else {
				hour := int(timeInt / 10000)
				minute := int((timeInt % 10000) / 100)
				second := int(timeInt % 100)
				t := fmt.Sprintf("%02d:%02d:%02d", hour, minute, second)
				row[column_name] = t
			}
			break

		case FIELD_TYPE_TIME2:
			var a byte
			var b byte
			var c byte
			binary.Read(buf, binary.BigEndian, &a)
			binary.Read(buf, binary.BigEndian, &b)
			binary.Read(buf, binary.BigEndian, &c)
			timeInt := uint64((int(a) << 16) | (int(b) << 8) | int(c))
			if timeInt >= 0x800000 {
				timeInt -= 0x1000000
			}
			hour := read_binary_slice(timeInt, 2, 10, 24)
			minute := read_binary_slice(timeInt, 12, 6, 24)
			second := read_binary_slice(timeInt, 18, 6, 24)
			t := fmt.Sprintf("%02d:%02d:%02d", hour, minute, second)
			fsp := tableMap.columnMetaData[i].fsp
			if fsp > 0 {
				nsec := readNsec(buf, fsp)
				nsec = nsec / mathPowerInt(10, int(6-fsp))
				timeFormat := "%02d:%02d:%02d.%0" + strconv.Itoa(int(fsp)) + "d"
				t = fmt.Sprintf(timeFormat, hour, minute, second, nsec)
			}

			row[column_name] = t
			break

		case FIELD_TYPE_TIMESTAMP:
			timestamp := int64(bytesToUint32(buf.Next(4)))
			if timestamp == 0 {
				row[column_name] = "0000-00-00 00:00:00"
				break
			}
			tm := time.Unix(timestamp, 0)
			row[column_name] = tm.Format(TIME_FORMAT)
			break

		case FIELD_TYPE_TIMESTAMP2:
			var timestamp int32
			binary.Read(buf, binary.BigEndian, &timestamp)
			fsp := tableMap.columnMetaData[i].fsp
			nsec := readNsec(buf, fsp)
			if timestamp == 0 {
				if fsp > 0 {
					row[column_name] = "0000-00-00 00:00:00." + fmt.Sprintf("%0*d", fsp, 0)
				} else {
					row[column_name] = "0000-00-00 00:00:00"
				}
				break
			}
			tm := time.Unix(int64(timestamp), int64(nsec)*1000)
			if fsp > 0 {
				row[column_name] = tm.Format(TIME_FORMAT + "." + fmt.Sprintf("%0*d", fsp, 0))
			} else {
				row[column_name] = tm.Format(TIME_FORMAT)
			}
			break
		case FIELD_TYPE_DATETIME:
			var t int64
			e = binary.Read(buf, binary.LittleEndian, &t)

			second := int(t % 100)
			minute := int((t % 10000) / 100)
			hour := int((t % 1000000) / 10000)

			d := int(t / 1000000)
			day := d % 100
			month := time.Month((d % 10000) / 100)
			year := d / 10000

			row[column_name] = time.Date(year, month, day, hour, minute, second, 0, time.UTC).Format(TIME_FORMAT)
			if row[column_name] == "-0001-11-30 00:00:00" {
				row[column_name] = "0000-00-00 00:00:00"
			}
			break

		case FIELD_TYPE_DATETIME2:
			row[column_name], e = read_datetime2(buf, tableMap.columnMetaData[i].fsp)
			break
		case FIELD_TYPE_JSON:
			var length uint64
			length, e = readFixedLengthInteger(buf, int(tableMap.columnMetaData[i].length_size))
			data := buf.Next(int(length))
			row[column_name], e = get_field_json_data(data, int64(length))
			break
		default:
			return nil, fmt.Errorf("schemaName:%s tableName:%s columnName:%s Unknown FieldType %d", tableMap.schemaName, tableMap.tableName, column_name, tableMap.columnTypes[i])
		}
		//log.Println("column_name:",column_name," row[column_name]:",row[column_name])
		if e != nil {
			log.Printf("lastField schemaName:%s tableName:%s columnName:%s Unknown FieldType %d err:%s", tableMap.schemaName, tableMap.tableName, column_name, tableMap.columnTypes[i], e)
			return nil, e
		}
		//log.Println("column_name:",column_name,"=",row[column_name])
	}
	return
}

func mathPowerInt(x int, n int) int {
	ans := 1
	for n != 0 {
		ans *= x
		n--
	}
	return ans
}

/*
Read a part of binary data and extract a number
binary: the data
start: From which bit (1 to X)
size: How many bits should be read
data_length: data size
*/
func read_binary_slice(binary uint64, start uint64, size uint64, data_length uint64) uint64 {
	binary = binary >> (data_length - (start + size))
	mask := (1 << size) - 1
	return binary & uint64(mask)
}

/*
DATETIME

1 bit  sign           (1= non-negative, 0= negative)
17 bits year*13+month  (year 0-9999, month 0-12)
5 bits day            (0-31)
5 bits hour           (0-23)
6 bits minute         (0-59)
6 bits second         (0-59)
---------------------------
40 bits = 5 bytes

*/
func read_datetime2(buf *bytes.Buffer, fsp uint8) (data string, err error) {
	defer func() {
		if errs := recover(); errs != nil {
			err = fmt.Errorf(fmt.Sprint(errs))
			return
		}
	}()
	var b byte
	var a uint32
	binary.Read(buf, binary.BigEndian, &a)
	binary.Read(buf, binary.BigEndian, &b)
	/*
		log.Println("read_datetime2 a:",a)
		log.Println("read_datetime2 b:",b)
		log.Println("a << 8:",uint(a) << 8)
	*/

	dataInt := uint64(b) + uint64((uint(a) << 8))
	year_month := read_binary_slice(dataInt, 1, 17, 40)
	year := int(year_month / 13)
	month := time.Month(year_month % 13)
	days := read_binary_slice(dataInt, 18, 5, 40)
	hours := read_binary_slice(dataInt, 23, 5, 40)
	minute := read_binary_slice(dataInt, 28, 6, 40)
	second := read_binary_slice(dataInt, 34, 6, 40)

	nsec := readNsec(buf, fsp)
	var timeFormat1 = TIME_FORMAT
	if fsp > 0 {
		timeFormat1 += "." + fmt.Sprintf("%0*d", fsp, 0)
	}
	data = time.Date(year, month, int(days), int(hours), int(minute), int(second), nsec*1000, time.UTC).Format(timeFormat1)
	if strings.Index(data, "-0001-11-30") == 0 {
		return strings.Replace(data, "-0001-11-30", "0000-00-00", 1), nil
	}
	return
}

/*

Fractional-part encoding depends on the fractional seconds precision (FSP).
// https://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
FSP	Storage
0 0 bytes
1,2 1 byte
3,4 2 bytes
4,5 3 bytes
*/
func readNsec(buf *bytes.Buffer, fsp uint8) int {
	// log.Println("read_datetime2 fsp:", fsp)

	nsec := 0
	switch fsp {
	case 1, 2:
		var c byte
		binary.Read(buf, binary.BigEndian, &c)
		// log.Println("read_datetime2 c:", c)

		nsec = int(uint(c) * 10000)
	case 3, 4:
		var c uint16
		binary.Read(buf, binary.BigEndian, &c)
		nsec = int(uint(c) * 100)
	case 5, 6:
		var cc byte
		var c uint16
		binary.Read(buf, binary.BigEndian, &c)
		binary.Read(buf, binary.BigEndian, &cc)
		// log.Println("read_datetime2 c:", c)
		// log.Println("read_datetime2 cc:", cc)

		nsec = int(uint(cc) + uint(c)<<8)
	}
	return nsec
}
