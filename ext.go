package dbr

func (this *Session) WithBlock(key string, blockValue string, blockSeconds int64) (bool, *Result) {
	var rResult = this.GET(key)
	if rResult.Error != nil {
		return false, rResult
	}

	if rResult.Data == nil {
		// 当从 redis 没有获取到数据的时候，写入 阻塞 数据
		this.SETEX(key, blockSeconds, blockValue)
		return false, nil
	}

	if rResult.MustString() == blockValue {
		// 当从 redis 获取到数据，并且数据等于 阻塞 数据的时候，返回阻塞
		return true, rResult
	}

	return false, rResult
}
