package dbr

import "encoding/json"

////////////////////////////////////////////////////////////////////////////////
// 把一个对象编码成 JSON 字符串数据进行存储
func (this *Session) MarshalJSON(key string, obj interface{}) (*Result) {
	value, err := json.Marshal(obj)
	if err != nil {
		return result(nil, err)
	}
	return this.SET(key, string(value))
}

func (this *Session) MarshalJSONEx(key string, seconds int, obj interface{}) (*Result) {
	value, err := json.Marshal(obj)
	if err != nil {
		return result(nil, err)
	}
	return this.SETEX(key, seconds, string(value))
}

func (this *Session) UnmarshalJSON(key string, des interface{}) (error) {
	var bs, err = this.GET(key).Bytes()
	if err != nil {
		return err
	}

	err = json.Unmarshal(bs, des)
	if err != nil {
		return err
	}

	return nil
}