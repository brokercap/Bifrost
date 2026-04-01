package mysql

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
)

const (
	JSONB_TYPE_SMALL_OBJECT = 0x0
	JSONB_TYPE_LARGE_OBJECT = 0x1
	JSONB_TYPE_SMALL_ARRAY  = 0x2
	JSONB_TYPE_LARGE_ARRAY  = 0x3
	JSONB_TYPE_LITERAL      = 0x4
	JSONB_TYPE_INT16        = 0x5
	JSONB_TYPE_UINT16       = 0x6
	JSONB_TYPE_INT32        = 0x7
	JSONB_TYPE_UINT32       = 0x8
	JSONB_TYPE_INT64        = 0x9
	JSONB_TYPE_UINT64       = 0xA
	JSONB_TYPE_DOUBLE       = 0xB
	JSONB_TYPE_STRING       = 0xC
	JSONB_TYPE_OPAQUE       = 0xF

	JSONB_LITERAL_NULL  = 0x0
	JSONB_LITERAL_TRUE  = 0x1
	JSONB_LITERAL_FALSE = 0x2

	// maxJSONElements caps object/array cardinality to avoid OOM on corrupt binlog or length mismatch.
	maxJSONElements = 10_000_000
)

type json_object_inlined_lengths_struct struct {
	x uint8
	y interface{}
	z interface{}
}

func get_field_json_data(data []byte, length int64) (interface{}, error) {
	if int64(len(data)) < length {
		length = int64(len(data))
	}
	if len(data) == 0 {
		return nil, nil
	}

	// Optimization: if first byte is not a valid JSONB type (0x00-0x0F),
	// it might be Partial JSON or text JSON.
	if data[0] > 0x0F {
		if v2, errP := parsePartialJSONBinlog(data); errP == nil {
			return v2, nil
		}
		if data[0] == '{' || data[0] == '[' {
			var j interface{}
			if errJ := json.Unmarshal(data, &j); errJ == nil {
				return j, nil
			}
		}
	}

	buf := bytes.NewBuffer(data)
	t, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	v, errBin := get_field_json_data0(buf, t, int64(buf.Len()), data)
	if errBin == nil {
		return v, nil
	}
	// MySQL 8.0 binlog_row_value_options=PARTIAL_JSON: column is diff blob, not full binary JSON.
	if v2, errP := parsePartialJSONBinlog(data); errP == nil {
		return v2, nil
	}

	if data[0] == '{' || data[0] == '[' {
		var j interface{}
		if errJ := json.Unmarshal(data, &j); errJ == nil {
			return j, nil
		}
	}

	return nil, errBin
}

func get_field_json_data0(buf *bytes.Buffer, t uint8, length int64, root []byte) (interface{}, error) {
	switch t {
	case JSONB_TYPE_SMALL_OBJECT, JSONB_TYPE_LARGE_OBJECT:
		var large bool
		if t == JSONB_TYPE_LARGE_OBJECT {
			large = true
		}
		return read_binary_json_object(buf, length, large, root)
	case JSONB_TYPE_SMALL_ARRAY, JSONB_TYPE_LARGE_ARRAY:
		var large bool
		// fix: should check LARGE_ARRAY (not LARGE_OBJECT)
		if t == JSONB_TYPE_LARGE_ARRAY {
			large = true
		}
		return read_binary_json_array(buf, length, large, root)
	case JSONB_TYPE_OPAQUE:
		return readJSONOpaque(buf)
	case JSONB_TYPE_STRING:
		return read_variable_length_string(buf), nil
	case JSONB_TYPE_LITERAL:
		value, e := buf.ReadByte()
		switch value {
		case JSONB_LITERAL_NULL:
			return nil, nil
		case JSONB_LITERAL_TRUE:
			return true, nil
		case JSONB_LITERAL_FALSE:
			return false, nil
		default:
			return nil, e
		}
	case JSONB_TYPE_INT16:
		var val int16
		e := binary.Read(buf, binary.LittleEndian, &val)
		return val, e
	case JSONB_TYPE_UINT16:
		var val uint16
		e := binary.Read(buf, binary.LittleEndian, &val)
		return val, e
	case JSONB_TYPE_INT32:
		var val int32
		e := binary.Read(buf, binary.LittleEndian, &val)
		return val, e
	case JSONB_TYPE_UINT32:
		var val uint32
		e := binary.Read(buf, binary.LittleEndian, &val)
		return val, e
	case JSONB_TYPE_INT64:
		var val int64
		e := binary.Read(buf, binary.LittleEndian, &val)
		return val, e
	case JSONB_TYPE_UINT64:
		var val uint64
		e := binary.Read(buf, binary.LittleEndian, &val)
		return val, e
	case JSONB_TYPE_DOUBLE:
		var double float64
		e := binary.Read(buf, binary.LittleEndian, &double)
		return double, e
	}

	return nil, fmt.Errorf("Json type %d is not handled", t)
}

// readJSONVaruintLength reads MySQL JSON variable-length prefix (same encoding as utf8mb4 string length).
// MySQL encodes this as up to 5 bytes (7 bits each), so bitsRead is capped at 35.
func readJSONVaruintLength(buf *bytes.Buffer) int {
	length := 0
	bitsRead := uint(0)
	for {
		if bitsRead > 35 {
			break
		}
		b, err := buf.ReadByte()
		if err != nil {
			break
		}
		length = length | ((int(b) & 0x7f) << bitsRead)
		bitsRead += 7
		if b&0x80 == 0 {
			break
		}
	}
	return length
}

// readJSONOpaque parses custom-data after type byte 0x0F: custom-type (enum_field_types) + varint length + payload.
func readJSONOpaque(buf *bytes.Buffer) (interface{}, error) {
	ft, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	n := readJSONVaruintLength(buf)
	if n > buf.Len() {
		n = buf.Len()
	}
	payload := make([]byte, n)
	if _, err := buf.Read(payload); err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"_json_opaque": true,
		"field_type":   int(ft),
		"payload_hex":  hex.EncodeToString(payload),
	}, nil
}

// parseJSONValueAtOffset parses a nested binary JSON value whose entry points into root (full column bytes; offsets match binlog / MySQL tests).
func parseJSONValueAtOffset(root []byte, off int64) (interface{}, error) {
	if off < 0 || int(off) >= len(root) {
		return nil, fmt.Errorf("json value offset %d out of range (len=%d)", off, len(root))
	}
	sub := root[off:]
	buf := bytes.NewBuffer(sub)
	t, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	return get_field_json_data0(buf, t, int64(buf.Len()), root)
}

// readMysqlNetFieldLength implements mysys/net_field_length (pack.cc) for binlog partial JSON path/data lengths.
func readMysqlNetFieldLength(buf *bytes.Buffer) (uint64, error) {
	b, err := buf.ReadByte()
	if err != nil {
		return 0, err
	}
	if b < 251 {
		return uint64(b), nil
	}
	if b == 251 {
		return 0, fmt.Errorf("unexpected NULL_LENGTH in net_field_length")
	}
	if b == 252 {
		var v uint16
		if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
			return 0, err
		}
		return uint64(v), nil
	}
	if b == 253 {
		bs := buf.Next(3)
		if len(bs) < 3 {
			return 0, io.ErrUnexpectedEOF
		}
		return uint64(bs[0]) | uint64(bs[1])<<8 | uint64(bs[2])<<16, nil
	}
	var v uint64
	if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
		return 0, err
	}
	return v, nil
}

// parsePartialJSONBinlog decodes MySQL 8.0 PARTIAL_JSON row image.
// Real layout per Json_diff_vector::read_binary() in mysql-server/sql/json_diff.cc:
//
//	uint32LE total-diff-bytes
//	repeated per diff:
//	  op (1 byte: 0=replace, 1=insert, 2=remove)
//	  path_len (LEB128)
//	  path (path_len bytes)
//	  [only for replace/insert:]
//	    value_len (LEB128)
//	    value     (value_len bytes, binary JSON)
func parsePartialJSONBinlog(data []byte) (interface{}, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("partial json too short")
	}
	total := int(binary.LittleEndian.Uint32(data[0:4]))
	if total < 0 || 4+total > len(data) {
		return nil, fmt.Errorf("partial json length out of range")
	}
	payload := data[4 : 4+total]
	buf := bytes.NewBuffer(payload)
	opNames := []string{"replace", "insert", "remove"}
	var diffs []map[string]interface{}
	for buf.Len() > 0 {
		op, err := buf.ReadByte()
		if err != nil {
			break
		}
		pathLen, err := readMysqlNetFieldLength(buf)
		if err != nil {
			return nil, err
		}
		if pathLen > uint64(buf.Len()) {
			return nil, fmt.Errorf("partial json path length overrun")
		}
		pathBytes := buf.Next(int(pathLen))
		if len(pathBytes) < int(pathLen) {
			return nil, io.ErrUnexpectedEOF
		}
		var val interface{}
		// op=2 (remove) carries no value
		if op != 2 {
			dataLen, err := readMysqlNetFieldLength(buf)
			if err != nil {
				return nil, err
			}
			if dataLen > uint64(buf.Len()) {
				return nil, fmt.Errorf("partial json data length overrun")
			}
			piece := buf.Next(int(dataLen))
			if len(piece) < int(dataLen) {
				return nil, io.ErrUnexpectedEOF
			}
			var errV error
			val, errV = get_field_json_data(piece, int64(len(piece)))
			if errV != nil {
				val = map[string]interface{}{"_parse_error": errV.Error(), "_raw_hex": hex.EncodeToString(piece)}
			}
		}
		opName := "unknown"
		if int(op) < len(opNames) {
			opName = opNames[op]
		}
		diffs = append(diffs, map[string]interface{}{
			"op":      int(op),
			"op_name": opName,
			"path":    string(pathBytes),
			"value":   val,
		})
	}
	return map[string]interface{}{
		"_binlog_partial_json": true,
		"diffs":                diffs,
	}, nil
}

func read_variable_length_string(buf *bytes.Buffer) string {
	/*
		Read a variable length string where the first 1-5 bytes stores the
		length of the string.

			For each byte, the first bit being high indicates another byte must be
		read.
	*/
	byte := 0x80
	length := 0
	bits_read := uint(0)
	for {
		if byte&0x80 != 0 {
			byte = int(buf.Next(1)[0])
			length = length | ((byte & 0x7f) << bits_read)
			bits_read = bits_read + 7
		} else {
			break
		}
	}
	if length > buf.Len() {
		length = buf.Len()
	}
	if length < 0 {
		length = 0
	}
	return string(buf.Next(length))
}

func read_binary_json_array(buf *bytes.Buffer, length int64, large bool, root []byte) (interface{}, error) {
	if rem := int64(buf.Len()); length > rem {
		length = rem
	}
	var elements int64
	var size int64

	if large {
		var elementsLarge, sizeLarge uint32
		binary.Read(buf, binary.LittleEndian, &elementsLarge)
		binary.Read(buf, binary.LittleEndian, &sizeLarge)
		elements = int64(elementsLarge)
		size = int64(sizeLarge)
	} else {
		var elementsSmall, sizeSmall uint16
		binary.Read(buf, binary.LittleEndian, &elementsSmall)
		binary.Read(buf, binary.LittleEndian, &sizeSmall)
		elements = int64(elementsSmall)
		size = int64(sizeSmall)
	}

	if size > length {
		err := fmt.Errorf("Json length: %d is larger than packet length %d", size, length)
		return nil, err
	}

	if elements > maxJSONElements {
		return nil, fmt.Errorf("Json array elements count %d exceeds max %d", elements, maxJSONElements)
	}

	// Validate elements count to prevent OOM from corrupted data
	// Each element requires at least 1 byte for type marker
	minNeededPerElement := int64(1)
	if elements > length/minNeededPerElement {
		err := fmt.Errorf("Json array elements count %d is impossible for data length %d", elements, length)
		return nil, err
	}

	values_type_offset_inline := make([]json_object_inlined_lengths_struct, elements)
	for i := int64(0); i < elements; i++ {
		values_type_offset_inline[i] = read_offset_or_inline(buf, large)
	}

	out := make([]interface{}, 0)
	for _, v := range values_type_offset_inline {
		var val interface{}
		if v.y == nil {
			val = v.z
		} else {
			offp, ok := v.y.(*int64)
			if !ok {
				return nil, fmt.Errorf("json array: unexpected offset type %T", v.y)
			}
			var err error
			val, err = parseJSONValueAtOffset(root, *offp)
			if err != nil {
				return nil, err
			}
		}
		out = append(out, val)
	}
	return out, nil
}

func read_binary_json_object(buf *bytes.Buffer, length int64, large bool, root []byte) (interface{}, error) {
	if rem := int64(buf.Len()); length > rem {
		length = rem
	}
	var elements int64
	var size int64

	if large {
		var elementsLarge, sizeLarge uint32
		binary.Read(buf, binary.LittleEndian, &elementsLarge)
		binary.Read(buf, binary.LittleEndian, &sizeLarge)
		elements = int64(elementsLarge)
		size = int64(sizeLarge)
	} else {
		var elementsSmall, sizeSmall uint16
		binary.Read(buf, binary.LittleEndian, &elementsSmall)
		binary.Read(buf, binary.LittleEndian, &sizeSmall)
		elements = int64(elementsSmall)
		size = int64(sizeSmall)
	}

	if size > length {
		err := fmt.Errorf("Json length: %d is larger than packet length %d", size, length)
		return nil, err
	}

	if elements > maxJSONElements {
		return nil, fmt.Errorf("Json object elements count %d exceeds max %d", elements, maxJSONElements)
	}

	// Validate elements count to prevent OOM from corrupted data
	// Each element requires at least 4 bytes (2 bytes offset + 2 bytes key length) in small format
	// or 6 bytes (4 bytes offset + 2 bytes key length) in large format
	minNeededPerElement := int64(4)
	if large {
		minNeededPerElement = 6
	}
	if elements > length/minNeededPerElement {
		err := fmt.Errorf("Json object elements count %d is impossible for data length %d", elements, length)
		return nil, err
	}

	key_offset_lengths := make([][]int64, elements)
	if large {
		for i := int64(0); i < elements; i++ {
			var x uint32
			var y uint16
			if err := binary.Read(buf, binary.LittleEndian, &x); err != nil {
				return nil, err
			}
			if err := binary.Read(buf, binary.LittleEndian, &y); err != nil {
				return nil, err
			}
			key_offset_lengths[i] = make([]int64, 2)
			key_offset_lengths[i][0] = int64(x)
			key_offset_lengths[i][1] = int64(y)
		}
	} else {
		var x uint16
		var y uint16
		for i := int64(0); i < elements; i++ {
			binary.Read(buf, binary.LittleEndian, &x)
			binary.Read(buf, binary.LittleEndian, &y)
			key_offset_lengths[i] = make([]int64, 2)
			key_offset_lengths[i][0] = int64(x)
			key_offset_lengths[i][1] = int64(y)
		}
	}

	value_type_inlined_lengths := make([]json_object_inlined_lengths_struct, elements)
	for i := int64(0); i < elements; i++ {
		value_type_inlined_lengths[i] = read_offset_or_inline(buf, large)
	}

	keys := make([]string, len(key_offset_lengths))
	for i, v := range key_offset_lengths {
		ko, kl := int(v[0]), int(v[1])
		if root != nil {
			if ko < 0 || kl < 0 || ko+kl > len(root) {
				return nil, fmt.Errorf("json key offset %d len %d out of range (root len=%d)", ko, kl, len(root))
			}
			keys[i] = string(root[ko : ko+kl])
		} else {
			keys[i] = string(buf.Next(kl))
		}
	}

	out := make(map[string]interface{}, 0)

	for i := int64(0); i < elements; i++ {
		var val interface{}
		if value_type_inlined_lengths[i].y == nil {
			val = value_type_inlined_lengths[i].z
		} else {
			offp, ok := value_type_inlined_lengths[i].y.(*int64)
			if !ok {
				return nil, fmt.Errorf("json object: unexpected offset type %T", value_type_inlined_lengths[i].y)
			}
			var err error
			val, err = parseJSONValueAtOffset(root, *offp)
			if err != nil {
				return nil, err
			}
		}
		out[keys[i]] = val
	}
	return out, nil
}

func read_offset_or_inline(buf *bytes.Buffer, large bool) (data json_object_inlined_lengths_struct) {
	data.x, _ = buf.ReadByte()
	switch data.x {
	case JSONB_TYPE_LITERAL, JSONB_TYPE_INT16, JSONB_TYPE_UINT16:
		// Always inlined: literal, int16, uint16
		data.y = nil
		data.z = read_binary_json_type_inlined(buf, data.x, large)
		return
	default:
		break
	}

	if large && (data.x == JSONB_TYPE_INT32 || data.x == JSONB_TYPE_UINT32) {
		data.y = nil
		z := read_binary_json_type_inlined(buf, data.x, large)
		if z == nil {
			data.z = nil
		} else {
			// read_binary_json_type_inlined returns int32 / uint32, not int64
			var z0 int64
			switch v := z.(type) {
			case int32:
				z0 = int64(v)
			case uint32:
				z0 = int64(v)
			case int64:
				z0 = v
			default:
				// unreachable: only INT32/UINT32 reach this branch
				data.z = nil
			}
			data.z = &z0
		}
		return
	}
	data.z = nil
	if large {
		// MySQL json_binary.h: large object/array uses uint32 for offset-or-inlined-value (not 8 bytes).
		var y uint32
		err := binary.Read(buf, binary.LittleEndian, &y)
		if err != nil {
			return json_object_inlined_lengths_struct{}
		}
		y0 := int64(y)
		data.y = &y0
	} else {
		var y uint16
		binary.Read(buf, binary.LittleEndian, &y)
		y0 := int64(y)
		data.y = &y0
	}
	//binary.Read(buf, binary.LittleEndian, data.y)
	return
}

func read_binary_json_type_inlined(buf *bytes.Buffer, z uint8, large bool) (data interface{}) {
	if z == JSONB_TYPE_LITERAL {
		var value uint32
		if large {
			binary.Read(buf, binary.LittleEndian, &value)
		} else {
			var smallValue uint16
			binary.Read(buf, binary.LittleEndian, &smallValue)
			value = uint32(smallValue)
		}
		if value == JSONB_LITERAL_NULL {
			data = nil
		}
		if value == JSONB_LITERAL_TRUE {
			data = true
		}
		if value == JSONB_LITERAL_FALSE {
			data = false
		}
		return
	}

	if z == JSONB_TYPE_INT16 {
		var value int16
		binary.Read(buf, binary.LittleEndian, &value)
		data = value
		return
	}
	if z == JSONB_TYPE_UINT16 {
		var value uint16
		binary.Read(buf, binary.LittleEndian, &value)
		data = value
		return
	}
	if z == JSONB_TYPE_INT32 {
		var value int32
		binary.Read(buf, binary.LittleEndian, &value)
		data = value
		return
	}
	if z == JSONB_TYPE_UINT32 {
		var value uint32
		binary.Read(buf, binary.LittleEndian, &value)
		data = value
		return
	}
	if z == JSONB_TYPE_INT64 {
		var value int64
		binary.Read(buf, binary.LittleEndian, &value)
		data = value
		return
	}
	if z == JSONB_TYPE_UINT64 {
		var value uint64
		binary.Read(buf, binary.LittleEndian, &value)
		data = value
		return
	}
	if z == JSONB_TYPE_DOUBLE {
		var value float64
		binary.Read(buf, binary.LittleEndian, &value)
		data = value
		return
	}

	// unreachable: caller (read_offset_or_inline) only passes inlinable types
	return nil
}
