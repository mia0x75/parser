// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"fmt"
	"math"
	"time"

	"github.com/mia0x75/parser/charset"
	"github.com/mia0x75/parser/mysql"
	"github.com/mia0x75/parser/terror"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
)

var _ = Suite(&testTypeConvertSuite{})

type testTypeConvertSuite struct {
}

type invalidMockType struct {
}

func (s *testTypeConvertSuite) TestConvertType(c *C) {
	ft := NewFieldType(mysql.TypeBlob)
	ft.Flen = 4
	ft.Charset = "utf8"
	v, err := Convert("123456", ft)
	c.Assert(ErrDataTooLong.Equal(err), IsTrue)
	c.Assert(v, Equals, "1234")
	ft = NewFieldType(mysql.TypeString)
	ft.Flen = 4
	ft.Charset = charset.CharsetBin
	v, err = Convert("12345", ft)
	c.Assert(ErrDataTooLong.Equal(err), IsTrue)
	c.Assert(v, DeepEquals, []byte("1234"))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(111.114, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, float32(111.11))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(999.999, ft)
	c.Assert(err, NotNil)
	c.Assert(v, Equals, float32(999.99))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(-999.999, ft)
	c.Assert(err, NotNil)
	c.Assert(v, Equals, float32(-999.99))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(1111.11, ft)
	c.Assert(err, NotNil)
	c.Assert(v, Equals, float32(999.99))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(999.916, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, float32(999.92))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(999.914, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, float32(999.91))

	ft = NewFieldType(mysql.TypeFloat)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(999.9155, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, float32(999.92))

	// For TypeBlob
	ft = NewFieldType(mysql.TypeBlob)
	_, err = Convert(&invalidMockType{}, ft)
	c.Assert(err, NotNil)

	// Nil
	ft = NewFieldType(mysql.TypeBlob)
	v, err = Convert(nil, ft)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	// TypeDouble
	ft = NewFieldType(mysql.TypeDouble)
	ft.Flen = 5
	ft.Decimal = 2
	v, err = Convert(999.9155, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, float64(999.92))

	// For TypeString
	ft = NewFieldType(mysql.TypeString)
	ft.Flen = 3
	v, err = Convert("12345", ft)
	c.Assert(ErrDataTooLong.Equal(err), IsTrue)
	c.Assert(v, Equals, "123")
	ft = NewFieldType(mysql.TypeString)
	ft.Flen = 3
	ft.Charset = charset.CharsetBin
	v, err = Convert("12345", ft)
	c.Assert(ErrDataTooLong.Equal(err), IsTrue)
	c.Assert(v, DeepEquals, []byte("123"))

	// For TypeDuration
	ft = NewFieldType(mysql.TypeDuration)
	ft.Decimal = 3
	v, err = Convert("10:11:12.123456", ft)
	c.Assert(err, IsNil)
	c.Assert(v.(Duration).String(), Equals, "10:11:12.123")
	ft.Decimal = 1
	vv, err := Convert(v, ft)
	c.Assert(err, IsNil)
	c.Assert(vv.(Duration).String(), Equals, "10:11:12.1")

	vd, err := ParseTime(nil, "2010-10-10 10:11:11.12345", mysql.TypeDatetime, 2)
	c.Assert(vd.String(), Equals, "2010-10-10 10:11:11.12")
	c.Assert(err, IsNil)
	v, err = Convert(vd, ft)
	c.Assert(err, IsNil)
	c.Assert(v.(Duration).String(), Equals, "10:11:11.1")

	// For mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDate
	ft = NewFieldType(mysql.TypeTimestamp)
	ft.Decimal = 3
	v, err = Convert("2010-10-10 10:11:11.12345", ft)
	c.Assert(err, IsNil)
	c.Assert(v.(Time).String(), Equals, "2010-10-10 10:11:11.123")
	ft.Decimal = 1
	vv, err = Convert(v, ft)
	c.Assert(err, IsNil)
	c.Assert(vv.(Time).String(), Equals, "2010-10-10 10:11:11.1")

	// For TypeLonglong
	ft = NewFieldType(mysql.TypeLonglong)
	v, err = Convert("100", ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(100))
	// issue 4287.
	v, err = Convert(math.Pow(2, 63)-1, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(math.MaxInt64))
	ft = NewFieldType(mysql.TypeLonglong)
	ft.Flag |= mysql.UnsignedFlag
	v, err = Convert("100", ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, uint64(100))
	// issue 3470
	ft = NewFieldType(mysql.TypeLonglong)
	v, err = Convert(Duration{Duration: time.Duration(12*time.Hour + 59*time.Minute + 59*time.Second + 555*time.Millisecond), Fsp: 3}, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(130000))
	v, err = Convert(Time{
		Time: FromDate(2017, 1, 1, 12, 59, 59, 555000),
		Type: mysql.TypeDatetime,
		Fsp:  MaxFsp}, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(20170101130000))

	// For TypeBit
	ft = NewFieldType(mysql.TypeBit)
	ft.Flen = 24 // 3 bytes.
	v, err = Convert("100", ft)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, NewBinaryLiteralFromUint(3223600, 3))

	v, err = Convert(NewBinaryLiteralFromUint(100, -1), ft)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, NewBinaryLiteralFromUint(100, 3))

	ft.Flen = 1
	v, err = Convert(1, ft)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, NewBinaryLiteralFromUint(1, 1))

	_, err = Convert(2, ft)
	c.Assert(err, NotNil)

	ft.Flen = 0
	_, err = Convert(2, ft)
	c.Assert(err, NotNil)

	// For TypeNewDecimal
	ft = NewFieldType(mysql.TypeNewDecimal)
	ft.Flen = 8
	ft.Decimal = 4
	v, err = Convert(3.1416, ft)
	c.Assert(err, IsNil, Commentf(errors.ErrorStack(err)))
	c.Assert(v.(*MyDecimal).String(), Equals, "3.1416")
	v, err = Convert("3.1415926", ft)
	c.Assert(terror.ErrorEqual(err, ErrTruncated), IsTrue, Commentf("err %v", err))
	c.Assert(v.(*MyDecimal).String(), Equals, "3.1416")
	v, err = Convert("99999", ft)
	c.Assert(terror.ErrorEqual(err, ErrOverflow), IsTrue, Commentf("err %v", err))
	c.Assert(v.(*MyDecimal).String(), Equals, "9999.9999")
	v, err = Convert("-10000", ft)
	c.Assert(terror.ErrorEqual(err, ErrOverflow), IsTrue, Commentf("err %v", err))
	c.Assert(v.(*MyDecimal).String(), Equals, "-9999.9999")

	// For TypeYear
	ft = NewFieldType(mysql.TypeYear)
	v, err = Convert("2015", ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(2015))
	v, err = Convert(2015, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(2015))
	_, err = Convert(1800, ft)
	c.Assert(err, NotNil)
	dt, err := ParseDate(nil, "2015-11-11")
	c.Assert(err, IsNil)
	v, err = Convert(dt, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(2015))
	v, err = Convert(ZeroDuration, ft)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(time.Now().Year()))

	// For enum
	ft = NewFieldType(mysql.TypeEnum)
	ft.Elems = []string{"a", "b", "c"}
	v, err = Convert("a", ft)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, Enum{Name: "a", Value: 1})
	v, err = Convert(2, ft)
	c.Log(errors.ErrorStack(err))
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, Enum{Name: "b", Value: 2})
	_, err = Convert("d", ft)
	c.Assert(err, NotNil)
	v, err = Convert(4, ft)
	c.Assert(terror.ErrorEqual(err, ErrTruncated), IsTrue, Commentf("err %v", err))
	c.Assert(v, DeepEquals, Enum{})

	ft = NewFieldType(mysql.TypeSet)
	ft.Elems = []string{"a", "b", "c"}
	v, err = Convert("a", ft)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, Set{Name: "a", Value: 1})
	v, err = Convert(2, ft)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, Set{Name: "b", Value: 2})
	v, err = Convert(3, ft)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, Set{Name: "a,b", Value: 3})
	_, err = Convert("d", ft)
	c.Assert(err, NotNil)
	_, err = Convert(9, ft)
	c.Assert(err, NotNil)
}

func testToString(c *C, val interface{}, expect string) {
	b, err := ToString(val)
	c.Assert(err, IsNil)
	c.Assert(b, Equals, expect)
}

func (s *testTypeConvertSuite) TestConvertToString(c *C) {
	testToString(c, "0", "0")
	testToString(c, true, "1")
	testToString(c, "false", "false")
	testToString(c, int(0), "0")
	testToString(c, int64(0), "0")
	testToString(c, uint64(0), "0")
	testToString(c, float32(1.6), "1.6")
	testToString(c, float64(-0.6), "-0.6")
	testToString(c, []byte{1}, "\x01")
	testToString(c, NewBinaryLiteralFromUint(0x4D7953514C, -1), "MySQL")
	testToString(c, NewBinaryLiteralFromUint(0x41, -1), "A")
	testToString(c, Enum{Name: "a", Value: 1}, "a")
	testToString(c, Set{Name: "a", Value: 1}, "a")

	td, err := ParseDuration(nil, "11:11:11.999999", 6)
	c.Assert(err, IsNil)
	testToString(c, td, "11:11:11.999999")

	ft := NewFieldType(mysql.TypeNewDecimal)
	ft.Flen = 10
	ft.Decimal = 5
	v, err := Convert(3.1415926, ft)
	c.Assert(terror.ErrorEqual(err, ErrTruncated), IsTrue, Commentf("err %v", err))
	testToString(c, v, "3.14159")

	_, err = ToString(&invalidMockType{})
	c.Assert(err, NotNil)
}

func (s *testTypeConvertSuite) TestStrToNum(c *C) {
	testStrToInt(c, "0", 0, true, nil)
	testStrToInt(c, "-1", -1, true, nil)
	testStrToInt(c, "100", 100, true, nil)
	testStrToInt(c, "65.0", 65, false, nil)
	testStrToInt(c, "65.0", 65, true, nil)
	testStrToInt(c, "", 0, false, nil)
	testStrToInt(c, "", 0, true, ErrTruncated)
	testStrToInt(c, "xx", 0, true, ErrTruncated)
	testStrToInt(c, "xx", 0, false, nil)
	testStrToInt(c, "11xx", 11, true, ErrTruncated)
	testStrToInt(c, "11xx", 11, false, nil)
	testStrToInt(c, "xx11", 0, false, nil)

	testStrToUint(c, "0", 0, true, nil)
	testStrToUint(c, "", 0, false, nil)
	testStrToUint(c, "", 0, false, nil)
	testStrToUint(c, "-1", 0xffffffffffffffff, false, ErrOverflow)
	testStrToUint(c, "100", 100, true, nil)
	testStrToUint(c, "+100", 100, true, nil)
	testStrToUint(c, "65.0", 65, true, nil)
	testStrToUint(c, "xx", 0, true, ErrTruncated)
	testStrToUint(c, "11xx", 11, true, ErrTruncated)
	testStrToUint(c, "xx11", 0, true, ErrTruncated)

	// TODO: makes StrToFloat return truncated value instead of zero to make it pass.
	testStrToFloat(c, "", 0, true, ErrTruncated)
	testStrToFloat(c, "-1", -1.0, true, nil)
	testStrToFloat(c, "1.11", 1.11, true, nil)
	testStrToFloat(c, "1.11.00", 1.11, false, nil)
	testStrToFloat(c, "1.11.00", 1.11, true, ErrTruncated)
	testStrToFloat(c, "xx", 0.0, false, nil)
	testStrToFloat(c, "0x00", 0.0, false, nil)
	testStrToFloat(c, "11.xx", 11.0, false, nil)
	testStrToFloat(c, "11.xx", 11.0, true, ErrTruncated)
	testStrToFloat(c, "xx.11", 0.0, false, nil)

	// for issue #5111
	testStrToFloat(c, "1e649", math.MaxFloat64, true, ErrTruncatedWrongVal)
	testStrToFloat(c, "1e649", math.MaxFloat64, false, nil)
	testStrToFloat(c, "-1e649", -math.MaxFloat64, true, ErrTruncatedWrongVal)
	testStrToFloat(c, "-1e649", -math.MaxFloat64, false, nil)
}

func (s *testTypeConvertSuite) TestFieldTypeToStr(c *C) {
	v := TypeToStr(mysql.TypeUnspecified, "not binary")
	c.Assert(v, Equals, TypeStr(mysql.TypeUnspecified))
	v = TypeToStr(mysql.TypeBlob, charset.CharsetBin)
	c.Assert(v, Equals, "blob")
	v = TypeToStr(mysql.TypeString, charset.CharsetBin)
	c.Assert(v, Equals, "binary")
}

func unsignedAccept(c *C, tp byte, value interface{}, expected string) {
	accept(c, tp, value, true, expected)
}

func signedAccept(c *C, tp byte, value interface{}, expected string) {
	accept(c, tp, value, false, expected)
}

func unsignedDeny(c *C, tp byte, value interface{}, expected string) {
	deny(c, tp, value, true, expected)
}

func signedDeny(c *C, tp byte, value interface{}, expected string) {
	deny(c, tp, value, false, expected)
}

func strvalue(v interface{}) string {
	return fmt.Sprintf("%v", v)
}

func (s *testTypeConvertSuite) TestConvert(c *C) {
	// integer ranges
	signedDeny(c, mysql.TypeTiny, -129, "-128")
	signedAccept(c, mysql.TypeTiny, -128, "-128")
	signedAccept(c, mysql.TypeTiny, 127, "127")
	signedDeny(c, mysql.TypeTiny, 128, "127")
	unsignedDeny(c, mysql.TypeTiny, -1, "255")
	unsignedAccept(c, mysql.TypeTiny, 0, "0")
	unsignedAccept(c, mysql.TypeTiny, 255, "255")
	unsignedDeny(c, mysql.TypeTiny, 256, "255")

	signedDeny(c, mysql.TypeShort, int64(math.MinInt16)-1, strvalue(int64(math.MinInt16)))
	signedAccept(c, mysql.TypeShort, int64(math.MinInt16), strvalue(int64(math.MinInt16)))
	signedAccept(c, mysql.TypeShort, int64(math.MaxInt16), strvalue(int64(math.MaxInt16)))
	signedDeny(c, mysql.TypeShort, int64(math.MaxInt16)+1, strvalue(int64(math.MaxInt16)))
	unsignedDeny(c, mysql.TypeShort, -1, "65535")
	unsignedAccept(c, mysql.TypeShort, 0, "0")
	unsignedAccept(c, mysql.TypeShort, uint64(math.MaxUint16), strvalue(uint64(math.MaxUint16)))
	unsignedDeny(c, mysql.TypeShort, uint64(math.MaxUint16)+1, strvalue(uint64(math.MaxUint16)))

	signedDeny(c, mysql.TypeInt24, -1<<23-1, strvalue(-1<<23))
	signedAccept(c, mysql.TypeInt24, -1<<23, strvalue(-1<<23))
	signedAccept(c, mysql.TypeInt24, 1<<23-1, strvalue(1<<23-1))
	signedDeny(c, mysql.TypeInt24, 1<<23, strvalue(1<<23-1))
	unsignedDeny(c, mysql.TypeInt24, -1, "16777215")
	unsignedAccept(c, mysql.TypeInt24, 0, "0")
	unsignedAccept(c, mysql.TypeInt24, 1<<24-1, strvalue(1<<24-1))
	unsignedDeny(c, mysql.TypeInt24, 1<<24, strvalue(1<<24-1))

	signedDeny(c, mysql.TypeLong, int64(math.MinInt32)-1, strvalue(int64(math.MinInt32)))
	signedAccept(c, mysql.TypeLong, int64(math.MinInt32), strvalue(int64(math.MinInt32)))
	signedAccept(c, mysql.TypeLong, int64(math.MaxInt32), strvalue(int64(math.MaxInt32)))
	signedDeny(c, mysql.TypeLong, uint64(math.MaxUint64), strvalue(uint64(math.MaxInt32)))
	signedDeny(c, mysql.TypeLong, int64(math.MaxInt32)+1, strvalue(int64(math.MaxInt32)))
	signedDeny(c, mysql.TypeLong, "1343545435346432587475", strvalue(int64(math.MaxInt32)))
	unsignedDeny(c, mysql.TypeLong, -1, "4294967295")
	unsignedAccept(c, mysql.TypeLong, 0, "0")
	unsignedAccept(c, mysql.TypeLong, uint64(math.MaxUint32), strvalue(uint64(math.MaxUint32)))
	unsignedDeny(c, mysql.TypeLong, uint64(math.MaxUint32)+1, strvalue(uint64(math.MaxUint32)))

	signedDeny(c, mysql.TypeLonglong, math.MinInt64*1.1, strvalue(int64(math.MinInt64)))
	signedAccept(c, mysql.TypeLonglong, int64(math.MinInt64), strvalue(int64(math.MinInt64)))
	signedAccept(c, mysql.TypeLonglong, int64(math.MaxInt64), strvalue(int64(math.MaxInt64)))
	signedDeny(c, mysql.TypeLonglong, math.MaxInt64*1.1, strvalue(int64(math.MaxInt64)))
	unsignedAccept(c, mysql.TypeLonglong, -1, "18446744073709551615")
	unsignedAccept(c, mysql.TypeLonglong, 0, "0")
	unsignedAccept(c, mysql.TypeLonglong, uint64(math.MaxUint64), strvalue(uint64(math.MaxUint64)))
	unsignedDeny(c, mysql.TypeLonglong, math.MaxUint64*1.1, strvalue(uint64(math.MaxUint64)))

	// integer from string
	signedAccept(c, mysql.TypeLong, "	  234  ", "234")
	signedAccept(c, mysql.TypeLong, " 2.35e3  ", "2350")
	signedAccept(c, mysql.TypeLong, " 2.e3  ", "2000")
	signedAccept(c, mysql.TypeLong, " -2.e3  ", "-2000")
	signedAccept(c, mysql.TypeLong, " 2e2  ", "200")
	signedAccept(c, mysql.TypeLong, " 0.002e3  ", "2")
	signedAccept(c, mysql.TypeLong, " .002e3  ", "2")
	signedAccept(c, mysql.TypeLong, " 20e-2  ", "0")
	signedAccept(c, mysql.TypeLong, " -20e-2  ", "0")
	signedAccept(c, mysql.TypeLong, " +2.51 ", "3")
	signedAccept(c, mysql.TypeLong, " -9999.5 ", "-10000")
	signedAccept(c, mysql.TypeLong, " 999.4", "999")
	signedAccept(c, mysql.TypeLong, " -3.58", "-4")
	signedDeny(c, mysql.TypeLong, " 1a ", "1")
	signedDeny(c, mysql.TypeLong, " +1+ ", "1")

	// integer from float
	signedAccept(c, mysql.TypeLong, 234.5456, "235")
	signedAccept(c, mysql.TypeLong, -23.45, "-23")
	unsignedAccept(c, mysql.TypeLonglong, 234.5456, "235")
	unsignedDeny(c, mysql.TypeLonglong, -23.45, "18446744073709551593")

	// float from string
	signedAccept(c, mysql.TypeFloat, "23.523", "23.523")
	signedAccept(c, mysql.TypeFloat, int64(123), "123")
	signedAccept(c, mysql.TypeFloat, uint64(123), "123")
	signedAccept(c, mysql.TypeFloat, int(123), "123")
	signedAccept(c, mysql.TypeFloat, float32(123), "123")
	signedAccept(c, mysql.TypeFloat, float64(123), "123")
	signedAccept(c, mysql.TypeDouble, " -23.54", "-23.54")
	signedDeny(c, mysql.TypeDouble, "-23.54a", "-23.54")
	signedDeny(c, mysql.TypeDouble, "-23.54e2e", "-2354")
	signedDeny(c, mysql.TypeDouble, "+.e", "0")
	signedAccept(c, mysql.TypeDouble, "1e+1", "10")

	// year
	signedDeny(c, mysql.TypeYear, 123, "<nil>")
	signedDeny(c, mysql.TypeYear, 3000, "<nil>")
	signedAccept(c, mysql.TypeYear, "2000", "2000")

	// time from string
	signedAccept(c, mysql.TypeDate, "2012-08-23", "2012-08-23")
	signedAccept(c, mysql.TypeDatetime, "2012-08-23 12:34:03.123456", "2012-08-23 12:34:03")
	signedAccept(c, mysql.TypeDatetime, ZeroDatetime, "0000-00-00 00:00:00")
	signedAccept(c, mysql.TypeDatetime, int64(0), "0000-00-00 00:00:00")
	signedAccept(c, mysql.TypeTimestamp, "2012-08-23 12:34:03.123456", "2012-08-23 12:34:03")
	signedAccept(c, mysql.TypeDuration, "10:11:12", "10:11:12")
	signedAccept(c, mysql.TypeDuration, ZeroDatetime, "00:00:00")
	signedAccept(c, mysql.TypeDuration, ZeroDuration, "00:00:00")
	signedAccept(c, mysql.TypeDuration, 0, "00:00:00")

	signedDeny(c, mysql.TypeDate, "2012-08-x", "0000-00-00")
	signedDeny(c, mysql.TypeDatetime, "2012-08-x", "0000-00-00 00:00:00")
	signedDeny(c, mysql.TypeTimestamp, "2012-08-x", "0000-00-00 00:00:00")
	signedDeny(c, mysql.TypeDuration, "2012-08-x", "00:00:00")

	// string from string
	signedAccept(c, mysql.TypeString, "abc", "abc")

	// string from integer
	signedAccept(c, mysql.TypeString, 5678, "5678")
	signedAccept(c, mysql.TypeString, ZeroDuration, "00:00:00")
	signedAccept(c, mysql.TypeString, ZeroDatetime, "0000-00-00 00:00:00")
	signedAccept(c, mysql.TypeString, []byte("123"), "123")

	//TODO add more tests
	signedAccept(c, mysql.TypeNewDecimal, 123, "123")
	signedAccept(c, mysql.TypeNewDecimal, int64(123), "123")
	signedAccept(c, mysql.TypeNewDecimal, uint64(123), "123")
	signedAccept(c, mysql.TypeNewDecimal, float32(123), "123")
	signedAccept(c, mysql.TypeNewDecimal, 123.456, "123.456")
	signedAccept(c, mysql.TypeNewDecimal, "-123.456", "-123.456")
	signedAccept(c, mysql.TypeNewDecimal, NewDecFromInt(12300000), "12300000")
	dec := NewDecFromInt(-123)
	dec.Shift(-5)
	dec.Round(dec, 5, ModeHalfEven)
	signedAccept(c, mysql.TypeNewDecimal, dec, "-0.00123")
}

func (s *testTypeConvertSuite) TestNumberToDuration(c *C) {
	var testCases = []struct {
		number int64
		fsp    int
		hasErr bool
		year   int
		month  int
		day    int
		hour   int
		minute int
		second int
	}{
		{20171222, 0, true, 0, 0, 0, 0, 0, 0},
		{171222, 0, false, 0, 0, 0, 17, 12, 22},
		{20171222020005, 0, false, 2017, 12, 22, 02, 00, 05},
		{10000000000, 0, true, 0, 0, 0, 0, 0, 0},
		{171222, 1, false, 0, 0, 0, 17, 12, 22},
		{176022, 1, true, 0, 0, 0, 0, 0, 0},
		{8391222, 1, true, 0, 0, 0, 0, 0, 0},
		{8381222, 0, false, 0, 0, 0, 838, 12, 22},
		{1001222, 0, false, 0, 0, 0, 100, 12, 22},
		{171260, 1, true, 0, 0, 0, 0, 0, 0},
	}

	for _, tc := range testCases {
		dur, err := NumberToDuration(tc.number, tc.fsp)
		if tc.hasErr {
			c.Assert(err, NotNil)
			continue
		}
		c.Assert(err, IsNil)
		c.Assert(dur.Hour(), Equals, tc.hour)
		c.Assert(dur.Minute(), Equals, tc.minute)
		c.Assert(dur.Second(), Equals, tc.second)
	}

	var testCases1 = []struct {
		number int64
		dur    time.Duration
	}{
		{171222, 17*time.Hour + 12*time.Minute + 22*time.Second},
		{-171222, -(17*time.Hour + 12*time.Minute + 22*time.Second)},
	}

	for _, tc := range testCases1 {
		dur, err := NumberToDuration(tc.number, 0)
		c.Assert(err, IsNil)
		c.Assert(dur.Duration, Equals, tc.dur)
	}
}
