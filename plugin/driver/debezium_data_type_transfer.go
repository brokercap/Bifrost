package driver

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type DebeziumJsonMsg struct {
	DebeziumParameters map[string]interface{}
	DebeziumVal        interface{}
	DebeziumType       string
	BifrostVal         interface{}
	BifrostFieldType   string
}

func (c *DebeziumJsonMsg) TransToGoTimestampFormatStr(str string) string {
	str, _ = strconv.Unquote(str)
	return strings.ReplaceAll(strings.ReplaceAll(strings.Trim(str, "\""), "T", " "), "Z", "")
}

func (c *DebeziumJsonMsg) ToBifrostTimestamp() (toVal interface{}, toFieldType string) {
	switch c.DebeziumType {
	case "int64":
		//1665857191098790
		if c.DebeziumVal != nil {
			tmpInt64, _ := strconv.ParseInt(c.DebeziumVal.(string), 10, 64)
			mSec := tmpInt64 % 1000000
			timer := time.Unix(tmpInt64/1000000, mSec).UTC()
			if mSec == 0 {
				toVal = timer.Format("2006-01-02 15:04:05")
				toFieldType = "datetime"
			} else {
				toVal = fmt.Sprintf("%s.%06d", timer.Format("2006-01-02 15:04:05"), mSec)
				toFieldType = "datetime(6)"
			}
		} else {
			toFieldType = "datetime(6)"
		}
	case "bytes":
		if c.DebeziumVal != nil {
			toVal = c.TransToGoTimestampFormatStr(c.DebeziumVal.(string))
		}
		toFieldType = "timestamp(6)"
	case "string":
		if c.DebeziumVal != nil {
			toVal = c.TransToGoTimestampFormatStr(c.DebeziumVal.(string))
		}
		toFieldType = "timestamp(6)"
	default:
		if c.DebeziumVal != nil {
			toVal = fmt.Sprint(c.DebeziumVal)
		}
		toFieldType = "text"
	}
	return
}

func (c *DebeziumJsonMsg) ToBifrostInt64() (toVal interface{}, toFieldType string) {
	if c.DebeziumVal != nil {
		toVal, _ = strconv.ParseInt(c.DebeziumVal.(string), 10, 64)
	}
	toFieldType = "int64"
	return
}

func (c *DebeziumJsonMsg) ToBifrostUint64() (toVal interface{}, toFieldType string) {
	if c.DebeziumVal != nil {
		toVal, _ = strconv.ParseUint(c.DebeziumVal.(string), 10, 64)
	}
	toFieldType = "uint64"
	return
}

func (c *DebeziumJsonMsg) ToBifrostInt32() (toVal interface{}, toFieldType string) {
	if c.DebeziumVal != nil {
		tmpInt, _ := strconv.ParseInt(c.DebeziumVal.(string), 10, 32)
		toVal = int32(tmpInt)
	}
	toFieldType = "int32"
	return
}

func (c *DebeziumJsonMsg) ToBifrostUint32() (toVal interface{}, toFieldType string) {
	if c.DebeziumVal != nil {
		tmpInt, _ := strconv.ParseUint(c.DebeziumVal.(string), 10, 32)
		toVal = uint32(tmpInt)
	}
	toFieldType = "uint32"
	return
}

func (c *DebeziumJsonMsg) ToBifrostInt16() (toVal interface{}, toFieldType string) {
	if c.DebeziumVal != nil {
		tmpInt, _ := strconv.Atoi(c.DebeziumVal.(string))
		toVal = int16(tmpInt)
	}
	toFieldType = "int16"
	return
}

func (c *DebeziumJsonMsg) ToBifrostUint16() (toVal interface{}, toFieldType string) {
	if c.DebeziumVal != nil {
		tmpInt, _ := strconv.Atoi(c.DebeziumVal.(string))
		toVal = uint16(tmpInt)
	}
	toFieldType = "uint16"
	return
}

func (c *DebeziumJsonMsg) ToBifrostInt8() (toVal interface{}, toFieldType string) {
	if c.DebeziumVal != nil {
		tmpInt, _ := strconv.Atoi(c.DebeziumVal.(string))
		toVal = int8(tmpInt)
	}
	toFieldType = "int8"
	return
}

func (c *DebeziumJsonMsg) ToBifrostUint8() (toVal interface{}, toFieldType string) {
	if c.DebeziumVal != nil {
		tmpInt, _ := strconv.Atoi(c.DebeziumVal.(string))
		toVal = uint8(tmpInt)
	}
	toFieldType = "uint8"
	return
}

func (c *DebeziumJsonMsg) ToBifrostJson() (toVal interface{}, toFieldType string) {
	toFieldType = "json"
	if c.DebeziumVal != nil {
		// string(json.RawMessage) 出来结果如下：
		// "{\"key1\":[2147483647,-2147483648,\"2\",null,true,922337203685477,-922337203685477,{\"key2\":\"qoY`uY,Np5Q\\\\OpX9&'o8试测测试据试数数数试试测据试测测\"},{\"key2\":false}]}"
		//toVal = strings.ReplaceAll(strings.Trim(toVal.(string), "\""), "\\\"", "\"")
		toVal, _ = strconv.Unquote(c.DebeziumVal.(string))
	}
	return
}

func (c *DebeziumJsonMsg) ToBifrostTime() (toVal interface{}, toFieldType string) {
	// 65191098000 ==> 18:06:31.098000
	// 65191 是当天第几秒
	if c.DebeziumVal != nil {
		tmpInt, _ := strconv.Atoi(c.DebeziumVal.(string))
		sec := tmpInt / 1000000
		hour := sec / 3600
		minute := sec % 3600 / 60
		second := sec % 60
		mSec := tmpInt % 1000000
		if mSec > 0 {
			// 没有办法区分是time(1) 或者是 time(6)，所有只要不是time 就全转成time(6)
			toVal = fmt.Sprintf("%02d:%02d:%02d.%06d", hour, minute, second, mSec)
			toFieldType = "time(6)"
		} else {
			toVal = fmt.Sprintf("%02d:%02d:%02d", hour, minute, second)
			toFieldType = "time"
		}
	} else {
		toFieldType = "time(6)"
	}
	return
}

func (c *DebeziumJsonMsg) ToBifrostDate() (toVal interface{}, toFieldType string) {
	// 19280 ==> 2022-10-15
	if c.DebeziumVal != nil {
		tmpInt64, _ := strconv.ParseInt(c.DebeziumVal.(string), 10, 32)
		now := time.Now()
		// 当时时间戳 / 86400 算出当前距离1970的天数，再和 目标值（date int）的中的数字 对比，相差了几天，然后进行相加减，再格式化
		toVal = now.AddDate(0, 0, int(tmpInt64-now.Unix()/86400)).Format("2006-01-02")
	}
	toFieldType = "date"
	return
}

func (c *DebeziumJsonMsg) ToBifrostDecimal() (toVal interface{}, toFieldType string) {
	if c.BifrostVal != nil {
		// string(json.RawMessage) 假如是字符串的情况下 是会前后增加引号的，比如 空字符串， string(json.RawMessage) == "\"\"" ,有个引号
		toVal, _ = strconv.Unquote(c.BifrostVal.(string))
	}
	var scale interface{}
	if c.DebeziumParameters != nil {
		if _, ok := c.DebeziumParameters["scale"]; ok {
			scale = c.DebeziumParameters["scale"]
		} else {
			// 假如没有 scale 则说明没有 precision,scale
			toFieldType = "decimal"
			return
		}
		toFieldType = fmt.Sprintf("decimal(%s,%s)", c.DebeziumParameters["connect.decimal.precision"], scale)
	} else {
		toFieldType = "decimal"
	}
	return
}

func (c *DebeziumJsonMsg) ToBifrostBits() (toVal interface{}, toFieldType string) {
	if c.BifrostVal != nil {
		toVal, _ = strconv.ParseInt(c.BifrostVal.(string), 10, 64)
	}
	toFieldType = "bit"
	return
}

func (c *DebeziumJsonMsg) ToBifrostDouble() (toVal interface{}, toFieldType string) {
	if c.BifrostVal != nil {
		toVal, _ = strconv.ParseFloat(c.DebeziumVal.(string), 64)
	}
	toFieldType = "double"
	return
}

func (c *DebeziumJsonMsg) ToBifrostFloat() (toVal interface{}, toFieldType string) {
	if c.BifrostVal != nil {
		float64Val, _ := strconv.ParseFloat(c.BifrostVal.(string), 32)
		toVal = float32(float64Val)
	}
	toFieldType = "float"
	return
}

func (c *DebeziumJsonMsg) ToBifrostYear() (toVal interface{}, toFieldType string) {
	if c.BifrostVal != nil {
		tmpInt, _ := strconv.ParseInt(c.BifrostVal.(string), 10, 32)
		toVal = int16(tmpInt)
	}
	toFieldType = "year"
	return
}

func (c *DebeziumJsonMsg) ToBifrostEnum() (toVal interface{}, toFieldType string) {
	if c.DebeziumParameters != nil && c.DebeziumParameters["allowed"] != "" {
		tmpArr := strings.Split(fmt.Sprint(c.DebeziumParameters["allowed"]), ",")
		toFieldType = fmt.Sprintf("enum('%s')", strings.Join(tmpArr, "','"))
	} else {
		toFieldType = "varchar(255)"
	}
	if c.DebeziumVal != nil {
		toVal, _ = strconv.Unquote(c.DebeziumVal.(string))
	}
	return
}

func (c *DebeziumJsonMsg) ToBifrostSet() (toVal interface{}, toFieldType string) {
	if c.DebeziumVal != nil {
		toVal, _ = strconv.Unquote(c.DebeziumVal.(string))
	}
	return toVal, "varchar(255)"
}

func (c *DebeziumJsonMsg) ToBifrostText() (toVal interface{}, toFieldType string) {
	if c.DebeziumVal != nil {
		toVal, _ = strconv.Unquote(c.DebeziumVal.(string))
	}
	return toVal, "text"
}

func (c *DebeziumJsonMsg) ToBifrostLongText() (toVal interface{}, toFieldType string) {
	if c.DebeziumVal != nil {
		toVal, _ = strconv.Unquote(c.DebeziumVal.(string))
	}
	return toVal, "longtext"
}
