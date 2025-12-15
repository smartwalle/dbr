package dbr

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

var uuidCounter uint64

func uuid() string {
	// 使用当前时间戳、进程ID和计数器确保唯一性
	var timestamp = time.Now().UnixNano()
	var pid = os.Getpid()
	var count = atomic.AddUint64(&uuidCounter, 1)

	// 生成随机字节增加随机性
	var randomBytes = make([]byte, 4)
	if _, err := rand.Read(randomBytes); err != nil {
		// 如果随机数生成失败，使用时间戳作为后备
		randomBytes[0] = byte(timestamp)
		randomBytes[1] = byte(timestamp >> 8)
		randomBytes[2] = byte(timestamp >> 16)
		randomBytes[3] = byte(timestamp >> 24)
	}

	// 组合所有部分生成唯一标识符
	data := make([]byte, 20)
	copy(data[0:8], fmt.Sprintf("%016x", timestamp))
	copy(data[8:12], fmt.Sprintf("%08x", pid))
	copy(data[12:16], fmt.Sprintf("%08x", count))
	copy(data[16:20], randomBytes)

	return hex.EncodeToString(data)
}
