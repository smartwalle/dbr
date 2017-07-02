package dbr

//HDEL 删除哈希表 key 中的一个或多个指定域，不存在的域将被忽略。
func (this *Session) HDEL(key string, fields ...string) (interface{}, error) {
	var ks []interface{}
	ks = append(ks, key)
	for _, f := range fields {
		ks = append(ks, f)
	}
	return this.Do("HDEL", ks...)
}

//HEXISTS 查看哈希表 key 中，给定域 field 是否存在。
func (this *Session) HEXISTS(key, field string) bool {
	return MustBool(this.Do("HEXISTS", key, field))
}

//HGET 返回哈希表 key 中给定域 field 的值。
func (this *Session) HGET(key string, field string) (interface{}, error) {
	return this.Do("HGET", key, field)
}

// HGETALL 返回哈希表 key 中，所有的域和值。
func (this *Session) HGETALL(key string) (interface{}, error) {
	return this.Do("HGETALL", key)
}

//HINCRBY 为哈希表 key 中的域 field 的值加上增量 increment 。
func (this *Session) HINCRBY(key, field string, increment int) (interface{}, error) {
	return this.Do("HINCRBY", key, field, increment)
}

//HINCRBYFLOAT 为哈希表 key 中的域 field 加上浮点数增量 increment 。
func (this *Session) HINCRBYFLOAT(key, field string, increment float64) (interface{}, error) {
	return this.Do("HINCRBYFLOAT", key, field, increment)
}

//HKEYS 返回哈希表 key 中的所有域。
func (this *Session) HKEYS(key string) (interface{}, error) {
	return this.Do("HKEYS", key)
}

//HLEN 返回哈希表 key 中域的数量。
func (this *Session) HLEN(key string) int64 {
	var r, _ = Int64(this.Do("HLEN", key))
	return r
}

//HMGET 返回哈希表 key 中，一个或多个给定域的值。
func (this *Session) HMGET(key string, fields ...string) (interface{}, error) {
	var ks []interface{}
	ks = append(ks, key)
	for _, f := range fields {
		ks = append(ks, f)
	}
	return this.Do("HMGET", ks...)
}

//HMSET 同时将多个 field-value (域-值)对设置到哈希表 key 中。
func (this *Session) HMSET(key string, params ...interface{}) (interface{}, error) {
	var ps []interface{}
	ps = append(ps, key)
	ps = append(ps, params...)
	return this.Do("HMSET", ps...)
}

func (this *Session) HMSETStruct(key string, obj interface{}) (interface{}, error) {
	return this.Do("HMSET", StructToArgs(key, obj)...)
}

//HSET 将哈希表 key 中的域 field 的值设为 value 。
func (this *Session) HSET(key, field string, value interface{}) (interface{}, error) {
	return this.Do("HSET", key, field, value)
}

//HSETNX 将哈希表 key 中的域 field 的值设置为 value ，当且仅当域 field 不存在。
func (this *Session) HSETNX(key, field string, value interface{}) (interface{}, error) {
	return this.Do("HSETNX", key, field, value)
}

//HVALS 返回哈希表 key 中所有域的值。
func (this *Session) HVALS(key string) (interface{}, error) {
	return this.Do("HVALS", key)
}

// HSTRLEN 返回哈希表 key 中， 与给定域 field 相关联的值的字符串长度（string length）。
func (this *Session) HSTRLEN(key, field string) (int64) {
	return MustInt64(this.Do("HSTRLEN", key, field))
}