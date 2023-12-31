package src

import (
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestParseDecimalDataType(t *testing.T) {
	Convey("Nullable(Decimal(9,2))", t, func() {
		toDataType := fmt.Sprintf("Nullable(Decimal(%d,%d))", 9, 2)
		decimal, err := ParseDecimalDataType(toDataType)
		So(err, ShouldBeNil)
		So(decimal.precision, ShouldEqual, 9)
		So(decimal.scale, ShouldEqual, 2)
		So(decimal.nobits, ShouldEqual, 32)
	})

	Convey("Decimal32", t, func() {
		toDataType := fmt.Sprintf("Decimal(%d,%d)", 9, 2)
		decimal, err := ParseDecimalDataType(toDataType)
		So(err, ShouldBeNil)
		So(decimal.precision, ShouldEqual, 9)
		So(decimal.scale, ShouldEqual, 2)
		So(decimal.nobits, ShouldEqual, 32)
	})

	Convey("Decimal64", t, func() {
		toDataType := fmt.Sprintf("Decimal(%d,%d)", 18, 2)
		decimal, err := ParseDecimalDataType(toDataType)
		So(err, ShouldBeNil)
		So(decimal.precision, ShouldEqual, 18)
		So(decimal.scale, ShouldEqual, 2)
		So(decimal.nobits, ShouldEqual, 64)
	})

	Convey("Decimal128", t, func() {
		toDataType := fmt.Sprintf("Decimal(%d,%d)", 38, 2)
		decimal, err := ParseDecimalDataType(toDataType)
		So(err, ShouldBeNil)
		So(decimal.precision, ShouldEqual, 38)
		So(decimal.scale, ShouldEqual, 2)
		So(decimal.nobits, ShouldEqual, 128)
	})

	Convey("Decimal256", t, func() {
		toDataType := fmt.Sprintf("Decimal(%d,%d)", 76, 1)
		decimal, err := ParseDecimalDataType(toDataType)
		So(err, ShouldBeNil)
		So(decimal.precision, ShouldEqual, 76)
		So(decimal.scale, ShouldEqual, 1)
		So(decimal.nobits, ShouldEqual, 256)
	})

	Convey("Decimal512", t, func() {
		toDataType := fmt.Sprintf("Decimal(%d,%d)", 80, 2)
		_, err := ParseDecimalDataType(toDataType)
		So(err, ShouldNotBeNil)
	})

	Convey("chType error", t, func() {
		_, err := ParseDecimalDataType("int")
		So(err, ShouldNotBeNil)

		_, err = ParseDecimalDataType("Decimal32(1)")
		So(err, ShouldNotBeNil)

		_, err = ParseDecimalDataType("Decimal(a,b)")
		So(err, ShouldNotBeNil)

		_, err = ParseDecimalDataType("Decimal(a,1)")
		So(err, ShouldNotBeNil)

		_, err = ParseDecimalDataType("Decimal(0,1)")
		So(err, ShouldNotBeNil)

		_, err = ParseDecimalDataType("Decimal(1,b)")
		So(err, ShouldNotBeNil)

		_, err = ParseDecimalDataType("Decimal(1,-1)")
		So(err, ShouldNotBeNil)
	})
}

func TestDecimal_ToData(t *testing.T) {
	Convey("Decimal32", t, func() {
		toDataType := fmt.Sprintf("Decimal(%d,%d)", 9, 2)
		decimal, err := ParseDecimalDataType(toDataType)
		So(err, ShouldBeNil)
		newData := decimal.ToData("3.6")
		So(newData, ShouldEqual, int32(360))
	})
	Convey("Decimal64", t, func() {
		toDataType := fmt.Sprintf("Decimal(%d,%d)", 18, 3)
		decimal, err := ParseDecimalDataType(toDataType)
		So(err, ShouldBeNil)
		newData := decimal.ToData("3.60")
		So(newData, ShouldEqual, int64(3600))
	})

	Convey("Decimal128", t, func() {
		toDataType := fmt.Sprintf("Decimal(%d,%d)", 39, 3)
		decimal, err := ParseDecimalDataType(toDataType)
		So(err, ShouldBeNil)
		newData := decimal.ToData("3.60")
		So(newData, ShouldEqual, "3.60")
	})

	Convey("Decimal256", t, func() {
		toDataType := fmt.Sprintf("Decimal(%d,%d)", 39, 3)
		decimal, err := ParseDecimalDataType(toDataType)
		So(err, ShouldBeNil)
		newData := decimal.ToData("3.60")
		So(newData, ShouldEqual, "3.60")
	})
}

func TestDecimalStrToInt64(t *testing.T) {
	casesList := []struct {
		src   string
		scale int
		to    int64
	}{
		{
			src:   "4.6",
			scale: 2,
			to:    460,
		},
		{
			src:   "4.0",
			scale: 2,
			to:    400,
		},
		{
			src:   "4.60",
			scale: 2,
			to:    460,
		},
		{
			src:   "4.06",
			scale: 2,
			to:    406,
		},
		{
			src:   "4.6000000",
			scale: 2,
			to:    460,
		},
		{
			src:   "4.6000006",
			scale: 2,
			to:    460,
		},
		{
			src:   "14.6000006",
			scale: 2,
			to:    1460,
		},
		{
			src:   "14.00000",
			scale: 2,
			to:    1400,
		},
		{
			src:   "16",
			scale: 2,
			to:    1600,
		},

		{
			src:   "4.6",
			scale: 1,
			to:    46,
		},
		{
			src:   "4.0",
			scale: 1,
			to:    40,
		},
		{
			src:   "4.60",
			scale: 1,
			to:    46,
		},
		{
			src:   "4.06",
			scale: 1,
			to:    40,
		},
		{
			src:   "4.6000000",
			scale: 1,
			to:    46,
		},
		{
			src:   "4.6000006",
			scale: 1,
			to:    46,
		},
		{
			src:   "14.6000006",
			scale: 1,
			to:    146,
		},
		{
			src:   "14.00000",
			scale: 1,
			to:    140,
		},
		{
			src:   "16",
			scale: 1,
			to:    160,
		},

		{
			src:   "4.6",
			scale: 0,
			to:    4,
		},
		{
			src:   "4.0",
			scale: 0,
			to:    4,
		},
		{
			src:   "4.60",
			scale: 0,
			to:    4,
		},
		{
			src:   "4.06",
			scale: 0,
			to:    4,
		},
		{
			src:   "4.6000000",
			scale: 0,
			to:    4,
		},
		{
			src:   "4.6000006",
			scale: 0,
			to:    4,
		},
		{
			src:   "14.6000006",
			scale: 0,
			to:    14,
		},
		{
			src:   "14.00000",
			scale: 0,
			to:    14,
		},
		{
			src:   "14",
			scale: 0,
			to:    14,
		},
		{
			src:   "-100.1",
			scale: 2,
			to:    -10010,
		},
		{
			src:   "-0.66",
			scale: 3,
			to:    -660,
		},
	}

	Convey("DecimalStrToInt64", t, func() {
		for _, caseInfo := range casesList {
			toDataType := fmt.Sprintf("Decimal(%d,%d)", 18, caseInfo.scale)
			toInt64 := InterfaceToDecimalData(caseInfo.src, toDataType)
			SoMsg(fmt.Sprintf("%s - scale:%d", caseInfo.src, caseInfo.scale), toInt64, ShouldEqual, caseInfo.to)
			return
		}
	})
}
