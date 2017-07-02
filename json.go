package dbr

import "encoding/json"

////////////////////////////////////////////////////////////////////////////////
// 把一个对象编码成 JSON 字符串数据进行存储
func (this *Session) SETJSON(key string, obj interface{}) (reply interface{}, err error) {
	value, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	reply, err = this.SET(key, string(value))

	return reply, err
}

func (this *Session) SETJSONEX(key string, seconds int, obj interface{}) (reply interface{}, err error) {
	value, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	reply, err = this.SETEX(key, seconds, string(value))
	return reply, err
}

func (this *Session) GETJSON(key string, destination interface{}) (error) {
	var bs, err = Bytes(this.GET(key))
	if err != nil {
		return err
	}

	err = json.Unmarshal(bs, destination)
	if err != nil {
		return err
	}

	return nil
}