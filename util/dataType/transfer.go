/*
Copyright [2018] [jc3wish]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package dataType

import (
	"math"
	"fmt"
)

type Type int16

const (
	BOOL_TYPE		Type = 1 << iota
	INT8_TYPE
	UINT8_TYPE
	INT16_TYPE
	UINT16_TYPE
	INT32_TYPE
	UINT32_TYPE
	INT64_TYPE
	UINT64_TYPE
	STRING_TYPE
	BYTE_TYPE
	BYTES_TYPE
	FLOAT32_TYPE
	FLOAT64_TYPE
)

func TransferDataType(data []byte,dataType Type)(v interface{},err error) {
	switch dataType {
	case BOOL_TYPE:
		if data[0] == 1{
			v = true
		}else{
			v = false
		}
		break
	case INT8_TYPE:
		v = int8(data[0])
		break
	case UINT8_TYPE:
		v = uint8(data[0])
		break
	case INT16_TYPE:
		v = BytesToInt16(data)
		break
	case UINT16_TYPE:
		v = BytesToUInt16(data)
		break
	case INT32_TYPE:
		v = BytesToInt32(data)
		break
	case UINT32_TYPE:
		v = BytesToUInt32(data)
		break
	case INT64_TYPE:
		v = BytesToInt64(data)
		break
	case UINT64_TYPE:
		v = BytesToUInt64(data)
		break
	case FLOAT32_TYPE:
		v = BytesToFloat32(data)
		break
	case FLOAT64_TYPE:
		v = BytesToFloat64(data)
		break
	case STRING_TYPE:
		v = string(data)
		break
	case BYTE_TYPE:
		v = data[0]
		break
	case BYTES_TYPE:
		v = data
		break
	default:
		v = nil
		err = fmt.Errorf("dataType not found")
	}
	return
}

func BytesToInt16(b []byte) (n int16) {
	for i := uint8(0); i < uint8(len(b)); i++ {
		n |= int16(b[i]) << (i * 8)
	}
	return
}

func BytesToUInt16(b []byte) (n uint16) {
	for i := uint8(0); i < uint8(len(b)); i++ {
		n |= uint16(b[i]) << (i * 8)
	}
	return
}


func BytesToInt32(b []byte) (n int32) {
	for i := uint8(0); i < uint8(len(b)); i++ {
		n |= int32(b[i]) << (i * 8)
	}
	return
}

func BytesToUInt32(b []byte) (n uint32) {
	for i := uint8(0); i < uint8(len(b)); i++ {
		n |= uint32(b[i]) << (i * 8)
	}
	return
}

func BytesToInt64(b []byte) (n int64) {
	for i := uint8(0); i < uint8(len(b)); i++ {
		n |= int64(b[i]) << (i * 8)
	}
	return
}

func BytesToUInt64(b []byte) (n uint64) {
	for i := uint8(0); i < uint8(len(b)); i++ {
		n |= uint64(b[i]) << (i * 8)
	}
	return
}

func BytesToFloat32(b []byte) (n float32) {
	return math.Float32frombits(BytesToUInt32(b))
}

func BytesToFloat64(b []byte) float64 {
	return math.Float64frombits(BytesToUInt64(b))
}