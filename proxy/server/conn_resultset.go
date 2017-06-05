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

	"github.com/flike/kingshard/core/errors"
	"github.com/flike/kingshard/core/hack"
	"github.com/flike/kingshard/mysql"
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
				} else {
					field := &mysql.Field{}
					r.Fields[j] = field
					r.FieldNames[string(r.Fields[j].Name)] = j
					field.Name = hack.Slice(names[j])
					if err = formatField(field, value); err != nil {
						return nil, err
					}
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
		_rowData, _ := v.ParseText(r.Fields)

		//开始过滤数据表中，敏感数据
		//手机号、姓名、身份证、银行卡号
		for i, f := range r.Fields {
			if (string(f.Table) == "admin_user" && string(f.Name) == "mobile") ||
				(string(f.Table) == "fh_user" && string(f.Name) == "u_mobile") ||
				(string(f.Table) == "fh_borrower_user" && string(f.Name) == "b_mobile") ||
				(string(f.Table) == "fh_warrant_user" && string(f.Name) == "mobile") ||
				(string(f.Table) == "fh_business_user" && string(f.Name) == "bu_mobile") ||
				(string(f.Table) == "fh_black_user" && string(f.Name) == "b_mobile") ||
				(string(f.Table) == "fh_bind_card_record" && string(f.Name) == "mobile") ||
				(string(f.Table) == "fh_order_customer_info" &&
					(string(f.Name) == "mobile" ||
						string(f.Name) == "link_mobile" ||
						string(f.Name) == "link2_mobile" ||
						string(f.Name) == "emergency_mobile" ||
						string(f.Name) == "link2_mate_mobile")) ||
				(string(f.Table) == "customer_verification" &&
					(string(f.Name) == "mobile" ||
						string(f.Name) == "emergency_mobile" ||
						string(f.Name) == "link2_mobile" ||
						string(f.Name) == "link_mobile" ||
						string(f.Name) == "link2_mate_mobile")) {
				//
				//fmt.Printf("%s.%s 过滤手机号 %s \n", string(f.Table), string(f.Name), _rowData[i])
				//
				v = append(v[0:3], []byte(ydyimportant.Mobile(_rowData[i].(string)))...)
			}
			if (string(f.Table) == "admin_user" && string(f.Name) == "id_card") ||
				(string(f.Table) == "fh_user" && string(f.Name) == "id_card") ||
				(string(f.Table) == "fh_borrower_user" && string(f.Name) == "b_id_card") ||
				(string(f.Table) == "fh_black_user" && string(f.Name) == "b_id_card") ||
				(string(f.Table) == "fh_bind_card_record" && string(f.Name) == "id_card") ||
				(string(f.Table) == "fh_order_customer_info" &&
					(string(f.Name) == "id_card" ||
						string(f.Name) == "link_id_card" ||
						string(f.Name) == "link2_id_card" ||
						string(f.Name) == "link2_mate_id_card")) {
				//
				//fmt.Printf("%s.%s 身份证 %s \n", string(f.Table), string(f.Name), _rowData[i])
				//
				v = append(v[0:3], []byte(ydyimportant.IdCard(_rowData[i].(string)))...)
			}
			if (string(f.Table) == "admin_user" && (string(f.Name) == "bank_card_one" || string(f.Name) == "bank_card_two")) ||
				(string(f.Table) == "fh_user" && string(f.Name) == "bank_card") ||
				(string(f.Table) == "fh_borrower_user" && string(f.Name) == "b_bank_card") ||
				(string(f.Table) == "fh_black_user" && string(f.Name) == "b_bank_card") ||
				(string(f.Table) == "fh_bind_card_record" && string(f.Name) == "bank_card") ||
				(string(f.Table) == "fh_order_customer_info" && string(f.Name) == "bank_card_id") {
				//
				//fmt.Printf("%s.%s 银行卡 %s \n", string(f.Table), string(f.Name), _rowData[i])
				//
				v = append(v[0:3], []byte(ydyimportant.IdCard(_rowData[i].(string)))...)
			}

		}

		data = data[0:4]
		data = append(data, v...)
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
