package mysql

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
)

type json_object_inlined_lengths_struct struct {
	x uint8
	y interface{}
	z interface{}
}

func get_field_json_data(data []byte, length int64) (interface{}, error) {
	buf := bytes.NewBuffer(data)
	t, _ := buf.ReadByte()
	return get_field_json_data0(buf, t, length)
}

func get_field_json_data0(buf *bytes.Buffer, t uint8, length int64) (interface{}, error) {
	switch t {
	case JSONB_TYPE_SMALL_OBJECT, JSONB_TYPE_LARGE_OBJECT:
		var large bool
		if t == JSONB_TYPE_LARGE_OBJECT {
			large = true
		}
		return read_binary_json_object(buf, length-1, large)
	case JSONB_TYPE_SMALL_ARRAY, JSONB_TYPE_LARGE_ARRAY:
		var large bool
		if t == JSONB_TYPE_LARGE_OBJECT {
			large = true
		}
		return read_binary_json_array(buf, length-1, large)
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
	return string(buf.Next(length))
}

func read_binary_json_array(buf *bytes.Buffer, length int64, large bool) (interface{}, error) {
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

	values_type_offset_inline := make([]json_object_inlined_lengths_struct, elements)
	for i := int64(0); i < elements; i++ {
		values_type_offset_inline[i] = read_offset_or_inline(buf, large)
	}

	out := make([]interface{}, 0)
	for _, v := range values_type_offset_inline {
		var val interface{}
		if v.y == nil {
			if v.z != nil {
				val = v.z
			} else {
				val = nil
			}
		} else {
			var err error
			val, err = get_field_json_data0(buf, v.x, length)
			if err != nil {
				return nil, err
			}
		}
		out = append(out, val)
	}
	return out, nil
}

func read_binary_json_object(buf *bytes.Buffer, length int64, large bool) (interface{}, error) {
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
	key_offset_lengths := make([][]int64, elements)
	if large {
		var x uint32
		var y uint16
		for i := int64(0); i < elements; i++ {
			binary.Read(buf, binary.LittleEndian, &x)
			binary.Read(buf, binary.LittleEndian, &y)
			key_offset_lengths[0] = make([]int64, 2)
			key_offset_lengths[0][0] = int64(x)
			key_offset_lengths[0][1] = int64(y)
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
		keys[i] = string(buf.Next(int(v[1])))
	}

	out := make(map[string]interface{}, 0)

	for i := int64(0); i < elements; i++ {
		var val interface{}
		if value_type_inlined_lengths[i].y == nil {
			if value_type_inlined_lengths[i].z != nil {
				val = value_type_inlined_lengths[i].z
			} else {
				val = nil
			}
		} else {
			x := value_type_inlined_lengths[i].x
			val, _ = get_field_json_data0(buf, x, length)
		}
		out[keys[i]] = val
	}
	return out, nil
}

func read_offset_or_inline(buf *bytes.Buffer, large bool) (data json_object_inlined_lengths_struct) {
	data.x, _ = buf.ReadByte()
	switch data.x {
	case JSONB_TYPE_LITERAL, JSONB_TYPE_INT16, JSONB_TYPE_UINT16:
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
			z0 := z.(int64)
			data.z = &z0
		}
		return
	}
	data.z = nil
	if large {
		binary.Read(buf, binary.LittleEndian, data.y)
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

	panic("Json type " + fmt.Sprint(z) + " is not handled")
}
