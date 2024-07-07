package src

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

type Decimal struct {
	nobits    int // its domain is {32, 64, 128}
	precision int
	scale     int
}

func ParseDecimalDataType(chType string) (decimal *Decimal, err error) {
	if strings.HasPrefix(chType, "Nullable(") {
		chType = chType[9 : len(chType)-1]
	}
	switch {
	case len(chType) < 12:
		fallthrough
	case !strings.HasPrefix(chType, "Decimal"):
		fallthrough
	case chType[7] != '(':
		fallthrough
	case chType[len(chType)-1] != ')':
		return nil, fmt.Errorf("invalid Decimal format: '%s'", chType)
	}

	var params = strings.Split(chType[8:len(chType)-1], ",")

	if len(params) != 2 {
		return nil, fmt.Errorf("invalid Decimal format: '%s'", chType)
	}

	params[0] = strings.TrimSpace(params[0])
	params[1] = strings.TrimSpace(params[1])

	decimal = &Decimal{}

	if decimal.precision, err = strconv.Atoi(params[0]); err != nil {
		return nil, fmt.Errorf("'%s' is not Decimal type: %s", chType, err)
	} else if decimal.precision < 1 {
		return nil, errors.New("wrong precision of Decimal type")
	}

	if decimal.scale, err = strconv.Atoi(params[1]); err != nil {
		return nil, fmt.Errorf("'%s' is not Decimal type: %s", chType, err)
	} else if decimal.scale < 0 || decimal.scale > decimal.precision {
		return nil, errors.New("wrong scale of Decimal type")
	}

	switch {
	case decimal.precision <= 9:
		decimal.nobits = 32
	case decimal.precision <= 18:
		decimal.nobits = 64
	case decimal.precision <= 38:
		decimal.nobits = 128
	case decimal.precision <= 76:
		decimal.nobits = 256
	default:
		return nil, errors.New("precision of Decimal exceeds max bound")
	}

	return decimal, nil
}

func (d *Decimal) ToData(v string) (to interface{}) {
	switch d.nobits {
	case 32:
		to = int32(d.ToInt64(v))
	case 64, 128:
		to = d.ToInt64(v)
	default:
		return v
	}
	return to
}

func (d *Decimal) ToInt64(v string) (decimalInt64 int64) {
	if d.scale == 0 {
		tmpArr := strings.Split(v, ".")
		decimalInt64, _ = strconv.ParseInt(tmpArr[0], 10, 64)
		return
	}
	var err error

	v = strings.TrimRight(strings.TrimRight(v, "0"), ".")
	tmpArr := strings.Split(v, ".")
	decimalInt64, err = strconv.ParseInt(strings.ReplaceAll(v, ".", ""), 10, 64)
	if err != nil {
		return 0
	}
	switch len(tmpArr) {
	case 1:
		decimalInt64 *= int64(math.Pow(10, float64(d.scale)))
	case 2:
		rightN := len(tmpArr[1])
		n := d.scale - rightN
		if n == 0 {
			return
		}
		if n > 0 {
			decimalInt64 *= int64(math.Pow(10, float64(n)))
		} else {
			decimalInt64 /= int64(math.Pow(10, float64(0-n)))
		}
	default:
		decimalInt64 = 0
	}
	return
}

func InterfaceToDecimalData(v interface{}, toDataType string) (to interface{}) {
	decimal, err := ParseDecimalDataType(toDataType)
	if err != nil {
		return v
	}
	return decimal.ToData(fmt.Sprint(v))
}
