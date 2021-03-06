// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package server

import (
	"fmt"
	"strconv"

	"github.com/qiwenilli/kingshard/core/errors"
	"github.com/qiwenilli/kingshard/core/hack"
	"github.com/qiwenilli/kingshard/mysql"
	"github.com/qiwenilli/ydyImportant"
)

var ydyimportant ydyImportant.Important

func formatValue(value interface{}) ([]byte, error) {
	if value == nil {
		return hack.Slice("NULL"), nil
	}
	switch v := value.(type) {
	case int8:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int16:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int32:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int64:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case uint8:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint16:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint32:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint64:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case float32:
		return strconv.AppendFloat(nil, float64(v), 'f', -1, 64), nil
	case float64:
		return strconv.AppendFloat(nil, float64(v), 'f', -1, 64), nil
	case []byte:
		return v, nil
	case string:
		return hack.Slice(v), nil
	default:
		return nil, fmt.Errorf("invalid type %T", value)
	}
}

func formatField(field *mysql.Field, value interface{}) error {
	switch value.(type) {
	case int8, int16, int32, int64, int:
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_LONGLONG
		field.Flag = mysql.BINARY_FLAG | mysql.NOT_NULL_FLAG
	case uint8, uint16, uint32, uint64, uint:
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_LONGLONG
		field.Flag = mysql.BINARY_FLAG | mysql.NOT_NULL_FLAG | mysql.UNSIGNED_FLAG
	case float32, float64:
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_DOUBLE
		field.Flag = mysql.BINARY_FLAG | mysql.NOT_NULL_FLAG
	case string, []byte:
		field.Charset = 33
		field.Type = mysql.MYSQL_TYPE_VAR_STRING
	default:
		return fmt.Errorf("unsupport type %T for resultset", value)
	}
	return nil
}

func (c *ClientConn) buildResultset(fields []*mysql.Field, names []string, values [][]interface{}) (*mysql.Resultset, error) {
	var ExistFields bool
	r := new(mysql.Resultset)

	r.Fields = make([]*mysql.Field, len(names))
	r.FieldNames = make(map[string]int, len(names))

	//use the field def that get from true database
	if len(fields) != 0 {
		if len(r.Fields) == len(fields) {
			ExistFields = true
		} else {
			return nil, errors.ErrInvalidArgument
		}
	}

	var b []byte
	var err error

	for i, vs := range values {
		if len(vs) != len(r.Fields) {
			return nil, fmt.Errorf("row %d has %d column not equal %d", i, len(vs), len(r.Fields))
		}

		var row []byte
		for j, value := range vs {
			//列的定义
			if i == 0 {
				if ExistFields {
					r.Fields[j] = fields[j]
					r.FieldNames[string(r.Fields[j].Name)] = j

					fmt.Println("...", j)

				} else {
					field := &mysql.Field{}
					r.Fields[j] = field
					r.FieldNames[string(r.Fields[j].Name)] = j
					field.Name = hack.Slice(names[j])
					if err = formatField(field, value); err != nil {
						return nil, err
					}

					//
					fmt.Println(j)
				}

			}
			b, err = formatValue(value)
			if err != nil {
				return nil, err
			}

			row = append(row, mysql.PutLengthEncodedString(b)...)
		}

		r.RowDatas = append(r.RowDatas, row)
	}
	//assign the values to the result
	r.Values = values

	return r, nil
}

func (c *ClientConn) writeResultset(status uint16, r *mysql.Resultset) error {
	c.affectedRows = int64(-1)
	total := make([]byte, 0, 4096)
	data := make([]byte, 4, 512)
	var err error

	columnLen := mysql.PutLengthEncodedInt(uint64(len(r.Fields)))

	data = append(data, columnLen...)
	total, err = c.writePacketBatch(total, data, false)
	if err != nil {
		return err
	}

	for _, v := range r.Fields {
		//
		data = data[0:4]
		data = append(data, v.Dump()...)
		total, err = c.writePacketBatch(total, data, false)
		if err != nil {
			return err
		}
	}

	total, err = c.writeEOFBatch(total, status, false)
	if err != nil {
		return err
	}

	for _, v := range r.RowDatas {

		//custome start
		//开始过滤数据表中，敏感数据
		//手机号、姓名、身份证、银行卡号
		_rowData, _ := v.ParseText(r.Fields)

		var row []byte

		var b []byte
		var err error

		for i, f := range r.Fields {

			//----------------------------------------------------------------------------
			if t := func(str string) bool {
				fieldList := []string{
					"mobile",
					"u_mobile",
					"b_mobile",
					"bu_mobile",
					"link_mobile",
					"link2_mobile",
					"emergency_mobile",
					"link2_mate_mobile",
					"customer_verification",
				}
				for _, _f := range fieldList {
					if str == _f {
						return true
					}
				}
				return false

			}(string(f.Name)); t {
				// fmt.Printf("%s.%s 过滤手机号 %s \n", string(f.Table), string(f.Name), ydyimportant.ToString(_rowData[i]))
				b, _ = formatValue(_rowData[i])
				b = []byte(ydyimportant.Mobile(string(b)))
				row = append(row, mysql.PutLengthEncodedString(b)...)

				continue
			}

			if t := func(str string) bool {
				fieldList := []string{
					//
					"bank_card_one",
					"bank_card_two",
					"bank_card",
					"b_bank_card",
				}
				for _, _f := range fieldList {
					if str == _f {
						return true
					}
				}
				return false

			}(string(f.Name)); t {
				//fmt.Printf("%s.%s 身份证 %s \n", string(f.Table), string(f.Name), _rowData[i])
				//
				b, _ = formatValue(_rowData[i])
				b = []byte(ydyimportant.IdCard(string(b)))
				row = append(row, mysql.PutLengthEncodedString(b)...)

				continue
			}

			if t := func(str string) bool {
				fieldList := []string{
					//
					"bank_card_one",
					"bank_card_two",
					"bank_card",
					"b_bank_card",
				}
				for _, _f := range fieldList {
					if str == _f {
						return true
					}
				}
				return false

			}(string(f.Name)); t {
				//fmt.Printf("%s.%s 银行卡 %s \n", string(f.Table), string(f.Name), _rowData[i])
				//
				b, _ = formatValue(_rowData[i])
				b = []byte(ydyimportant.BankCard(string(b)))
				row = append(row, mysql.PutLengthEncodedString(b)...)

				continue
			}
			//----------------------------------------------------------------------------

			//
			b, _ = formatValue(_rowData[i])
			row = append(row, mysql.PutLengthEncodedString(b)...)
		}
		//custome end

		data = data[0:4]
		data = append(data, row...)
		total, err = c.writePacketBatch(total, data, false)
		if err != nil {
			return err
		}
	}

	total, err = c.writeEOFBatch(total, status, true)
	total = nil
	if err != nil {
		return err
	}

	return nil
}
