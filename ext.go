package dbr

import "time"

func (this *Session) WithBlock(key string, blockValue string, blockSeconds int64) (bool, *Result) {
	var rResult = this.GET(key)
	if rResult.Error != nil {
		return false, rResult
	}

	if rResult.Data == nil {
		// 当从 redis 没有获取到数据的时候，写入 阻塞 数据
		if this.SETNX(key, blockValue).MustInt() == 1 {
			this.EXPIRE(key, blockSeconds)
			return false, nil
		}
		time.Sleep(time.Millisecond * 500)
		return this.WithBlock(key, blockValue, blockSeconds)
	}

	if rResult.MustString() == blockValue {
		// 当从 redis 获取到数据，并且数据等于 阻塞 数据的时候，返回阻塞
		return true, rResult
	}

	return false, rResult
}
