package mysql

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// 创建一个模拟的 JSONB_TYPE_LARGE_ARRAY 数据
func createMockLargeArrayData() []byte {
	buf := bytes.NewBuffer(nil)

	// 写入类型标识 JSONB_TYPE_LARGE_ARRAY
	buf.WriteByte(JSONB_TYPE_LARGE_ARRAY)

	// 写入元素数量 (uint32) 和大小 (uint32)
	elements := uint32(2)
	size := uint32(15) // 修正大小: 8(header) + 3(first element) + 4(second element)
	binary.Write(buf, binary.LittleEndian, elements)
	binary.Write(buf, binary.LittleEndian, size)

	// 写入元素类型和偏移信息
	// 第一个元素: INT16 类型，内联
	buf.WriteByte(JSONB_TYPE_INT16)
	var val1 int16 = 42
	binary.Write(buf, binary.LittleEndian, val1)

	// 第二个元素: LITERAL 类型，内联
	buf.WriteByte(JSONB_TYPE_LITERAL)
	var literal uint32 = JSONB_LITERAL_TRUE
	binary.Write(buf, binary.LittleEndian, literal)

	return buf.Bytes()
}

// 创建一个模拟的 JSONB_TYPE_LARGE_OBJECT 数据
func createMockLargeObjectData() []byte {
	buf := bytes.NewBuffer(nil)

	// 写入类型标识 JSONB_TYPE_LARGE_OBJECT
	buf.WriteByte(JSONB_TYPE_LARGE_OBJECT)

	// 写入元素数量 (uint32) 和大小 (uint32)
	elements := uint32(2)
	// size = bytes of object body after root type byte (MySQL json_binary): header+meta+values+keys
	size := uint32(35)
	binary.Write(buf, binary.LittleEndian, elements)
	binary.Write(buf, binary.LittleEndian, size)

	// 写入 key 偏移和长度信息 (large format)
	// key-offset 为相对整列二进制**从首字节起**的绝对偏移（与 MySQL binlog 一致）
	// 首字节 type(1) + header(8) + key-meta(12) + value-meta(8) = 29 起为 "name"
	var keyOffset1 uint32 = 29
	var keyLen1 uint16 = 4
	binary.Write(buf, binary.LittleEndian, keyOffset1)
	binary.Write(buf, binary.LittleEndian, keyLen1)

	var keyOffset2 uint32 = 33
	var keyLen2 uint16 = 3
	binary.Write(buf, binary.LittleEndian, keyOffset2)
	binary.Write(buf, binary.LittleEndian, keyLen2)

	// 写入值类型和偏移信息
	// 第一个值: INT16 类型，内联
	buf.WriteByte(JSONB_TYPE_INT16)
	var val1 int16 = 100
	binary.Write(buf, binary.LittleEndian, val1)

	// 第二个值: LITERAL 类型，内联
	buf.WriteByte(JSONB_TYPE_LITERAL)
	var literal uint32 = JSONB_LITERAL_FALSE
	binary.Write(buf, binary.LittleEndian, literal)

	// 写入 key 字符串
	buf.WriteString("name") // 4 bytes
	buf.WriteString("age")  // 3 bytes

	return buf.Bytes()
}

// 测试 LARGE_ARRAY 类型判断修复
func TestLargeArrayTypeFix(t *testing.T) {
	data := createMockLargeArrayData()

	result, err := get_field_json_data(data, int64(len(data)))
	if err != nil {
		t.Fatalf("Failed to parse large array: %v", err)
	}

	// 验证结果是数组类型
	arr, ok := result.([]interface{})
	if !ok {
		t.Fatalf("Expected array, got %T", result)
	}

	// 验证数组长度
	if len(arr) != 2 {
		t.Fatalf("Expected 2 elements, got %d", len(arr))
	}

	// 验证第一个元素
	if arr[0] != int16(42) {
		t.Fatalf("Expected first element to be 42, got %v", arr[0])
	}

	// 验证第二个元素
	if arr[1] != true {
		t.Fatalf("Expected second element to be true, got %v", arr[1])
	}

	t.Logf("Large array parsed successfully: %+v", result)
}

// 测试 LARGE_OBJECT key offset 索引修复
func TestLargeObjectKeyOffsetFix(t *testing.T) {
	data := createMockLargeObjectData()

	result, err := get_field_json_data(data, int64(len(data)))
	if err != nil {
		t.Fatalf("Failed to parse large object: %v", err)
	}

	// 验证结果是对象类型
	obj, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected object, got %T", result)
	}

	// 验证对象有两个键
	if len(obj) != 2 {
		t.Fatalf("Expected 2 keys, got %d", len(obj))
	}

	// 验证键值对
	if obj["name"] != int16(100) {
		t.Fatalf("Expected 'name' to be 100, got %v", obj["name"])
	}

	if obj["age"] != false {
		t.Fatalf("Expected 'age' to be false, got %v", obj["age"])
	}

	t.Logf("Large object parsed successfully: %+v", result)
}

// 测试小数组和小对象（确保没有破坏原有功能）
func TestSmallArrayAndObject(t *testing.T) {
	// 测试小数组
	smallArrayBuf := bytes.NewBuffer(nil)
	smallArrayBuf.WriteByte(JSONB_TYPE_SMALL_ARRAY)

	elements := uint16(1)
	size := uint16(7) // 修正大小: 4(header) + 3(element: 1 byte type + 2 bytes value)
	binary.Write(smallArrayBuf, binary.LittleEndian, elements)
	binary.Write(smallArrayBuf, binary.LittleEndian, size)

	// 一个 INT16 元素
	smallArrayBuf.WriteByte(JSONB_TYPE_INT16)
	var val int16 = 123
	binary.Write(smallArrayBuf, binary.LittleEndian, val)

	result, err := get_field_json_data(smallArrayBuf.Bytes(), int64(smallArrayBuf.Len()))
	if err != nil {
		t.Fatalf("Failed to parse small array: %v", err)
	}

	arr, ok := result.([]interface{})
	if !ok || len(arr) != 1 || arr[0] != int16(123) {
		t.Fatalf("Small array parsing failed: %v", result)
	}

	t.Logf("Small array parsed successfully: %+v", result)
}

// 边界测试：测试空数组和空对象
func TestEmptyArrayAndObject(t *testing.T) {
	// 测试空的大数组
	emptyArrayBuf := bytes.NewBuffer(nil)
	emptyArrayBuf.WriteByte(JSONB_TYPE_LARGE_ARRAY)

	elements := uint32(0)
	size := uint32(8) // 仅包含头部信息
	binary.Write(emptyArrayBuf, binary.LittleEndian, elements)
	binary.Write(emptyArrayBuf, binary.LittleEndian, size)

	result, err := get_field_json_data(emptyArrayBuf.Bytes(), int64(emptyArrayBuf.Len()))
	if err != nil {
		t.Fatalf("Failed to parse empty large array: %v", err)
	}

	arr, ok := result.([]interface{})
	if !ok || len(arr) != 0 {
		t.Fatalf("Empty large array parsing failed: %v", result)
	}

	t.Logf("Empty large array parsed successfully: %+v", result)
}

// large 格式下内联 UINT32（曾错误断言为 int64 导致 panic）
func createMockLargeArrayInlinedUint32() []byte {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(JSONB_TYPE_LARGE_ARRAY)
	elements := uint32(1)
	// 8(header) + 1(type) + 4(uint32 value) = 13
	size := uint32(13)
	binary.Write(buf, binary.LittleEndian, elements)
	binary.Write(buf, binary.LittleEndian, size)
	buf.WriteByte(JSONB_TYPE_UINT32)
	var u uint32 = 0xDEADBEEF
	binary.Write(buf, binary.LittleEndian, u)
	return buf.Bytes()
}

func TestLargeArrayInlinedUint32(t *testing.T) {
	data := createMockLargeArrayInlinedUint32()
	result, err := get_field_json_data(data, int64(len(data)))
	if err != nil {
		t.Fatalf("parse large array with inlined uint32: %v", err)
	}
	arr, ok := result.([]interface{})
	if !ok || len(arr) != 1 {
		t.Fatalf("expected 1-element array, got %T %+v", result, result)
	}
	p, ok := arr[0].(*int64)
	if !ok {
		t.Fatalf("expected *int64 (large inlined int32/uint32), got %T %v", arr[0], arr[0])
	}
	if *p != int64(0xDEADBEEF) {
		t.Fatalf("expected 0xDEADBEEF, got %d", *p)
	}
}

// large 格式下内联 INT32
func createMockLargeArrayInlinedInt32() []byte {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(JSONB_TYPE_LARGE_ARRAY)
	elements := uint32(1)
	size := uint32(13)
	binary.Write(buf, binary.LittleEndian, elements)
	binary.Write(buf, binary.LittleEndian, size)
	buf.WriteByte(JSONB_TYPE_INT32)
	var i int32 = -123456789
	binary.Write(buf, binary.LittleEndian, i)
	return buf.Bytes()
}

func TestLargeArrayInlinedInt32(t *testing.T) {
	data := createMockLargeArrayInlinedInt32()
	result, err := get_field_json_data(data, int64(len(data)))
	if err != nil {
		t.Fatalf("parse large array with inlined int32: %v", err)
	}
	arr, ok := result.([]interface{})
	if !ok || len(arr) != 1 {
		t.Fatalf("expected 1-element array, got %T %+v", result, result)
	}
	p, ok := arr[0].(*int64)
	if !ok {
		t.Fatalf("expected *int64, got %T", arr[0])
	}
	if *p != -123456789 {
		t.Fatalf("expected -123456789, got %d", *p)
	}
}

// large object 下单个 key，值为内联 UINT32
func createMockLargeObjectInlinedUint32() []byte {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(JSONB_TYPE_LARGE_OBJECT)
	elements := uint32(1)
	// 1(type)+8(header)+6(key meta)+5(value UINT32 large inline)+1("k")=21; body size after type=20
	size := uint32(20)
	binary.Write(buf, binary.LittleEndian, elements)
	binary.Write(buf, binary.LittleEndian, size)
	var keyOff uint32 = 20
	var keyLen uint16 = 1
	binary.Write(buf, binary.LittleEndian, keyOff)
	binary.Write(buf, binary.LittleEndian, keyLen)
	buf.WriteByte(JSONB_TYPE_UINT32)
	var u uint32 = 42
	binary.Write(buf, binary.LittleEndian, u)
	buf.WriteString("k")
	return buf.Bytes()
}

func TestLargeObjectInlinedUint32(t *testing.T) {
	data := createMockLargeObjectInlinedUint32()
	result, err := get_field_json_data(data, int64(len(data)))
	if err != nil {
		t.Fatalf("parse large object with inlined uint32: %v", err)
	}
	obj := result.(map[string]interface{})
	p := obj["k"].(*int64)
	if *p != 42 {
		t.Fatalf("expected 42, got %d", *p)
	}
}

// Binlog may declare a JSON length larger than bytes actually present (Next() truncates).
// Parser must not use the declared length alone or makeslice can OOM on corrupt metadata.
func TestJSONDeclaredLengthLargerThanPayloadNoOOM(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(JSONB_TYPE_LARGE_OBJECT)
	// Absurd counts with only header bytes — would pass old length-based check if length were huge.
	binary.Write(buf, binary.LittleEndian, uint32(0x7fffffff))
	binary.Write(buf, binary.LittleEndian, uint32(8))
	data := buf.Bytes()
	_, err := get_field_json_data(data, 1<<30)
	if err == nil {
		t.Fatal("expected parse error for corrupt/truncated JSON payload")
	}
}

// MySQL 8 PARTIAL_JSON row image: LE32 total length then (op, path_len, path, [data_len, data])*.
func TestParsePartialJSONBinlog(t *testing.T) {
	body := bytes.NewBuffer(nil)
	body.WriteByte(2) // remove
	body.WriteByte(7) // path len < 251
	body.WriteString(`$.field`)
	total := uint32(body.Len())
	all := bytes.NewBuffer(nil)
	if err := binary.Write(all, binary.LittleEndian, total); err != nil {
		t.Fatal(err)
	}
	all.Write(body.Bytes())
	if all.Len() != 4+int(total) {
		t.Fatalf("test layout: len=%d want %d", all.Len(), 4+int(total))
	}
	v, err := parsePartialJSONBinlog(all.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	m, ok := v.(map[string]interface{})
	if !ok || m["_binlog_partial_json"] != true {
		t.Fatalf("expected partial wrapper map, got %T %+v", v, v)
	}
	diffs := m["diffs"].([]map[string]interface{})
	if len(diffs) != 1 || diffs[0]["op_name"] != "remove" || diffs[0]["path"] != `$.field` {
		t.Fatalf("unexpected diffs: %+v", diffs)
	}
}

func TestParsePartialJSONBinlogLengthMismatch(t *testing.T) {
	// total claims 10 bytes follow but buffer is shorter — must not accept as partial
	b := []byte{10, 0, 0, 0, 1, 2, 3}
	_, err := parsePartialJSONBinlog(b)
	if err == nil {
		t.Fatal("expected error for length mismatch")
	}
}

func TestParsePartialJSONBinlogPadded(t *testing.T) {
	// total claims 1 byte follows, but we provide 10 bytes. should succeed and ignore padding.
	body := bytes.NewBuffer(nil)
	body.WriteByte(2) // remove op
	path := "$.a"
	body.WriteByte(byte(len(path)))
	body.WriteString(path)

	total := uint32(body.Len())
	all := bytes.NewBuffer(nil)
	binary.Write(all, binary.LittleEndian, total)
	all.Write(body.Bytes())

	// Add 10 bytes of padding
	all.Write(make([]byte, 10))

	v, err := parsePartialJSONBinlog(all.Bytes())
	if err != nil {
		t.Fatalf("failed to parse padded partial json: %v", err)
	}

	m, ok := v.(map[string]interface{})
	if !ok || m["_binlog_partial_json"] != true {
		t.Fatalf("expected partial wrapper map, got %T %+v", v, v)
	}
	diffs := m["diffs"].([]map[string]interface{})
	if len(diffs) != 1 || diffs[0]["path"] != path {
		t.Fatalf("unexpected diffs: %+v", diffs)
	}
}
