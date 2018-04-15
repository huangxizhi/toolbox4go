package toolbox4go

import (
	"testing"
)

var (
	test_ip string = "localhost"
	test_port string = "6379"
	test_db int  = 0
)

func getRedisClinet() *RedisClient {
	return NewRedisClient(test_ip, test_port, test_db)
}

func TestSet(t *testing.T) {
	key, val := "name", "redis"
	_, err := getRedisClinet().Set(key, val)
	if err != nil {
		t.Errorf("set err:%v", err)
	}

	rVal, err := getRedisClinet().Get(key)
	if err != nil {
		t.Errorf("get err:%v", err)
	}
	if rVal != val {
		t.Errorf("set and get not the save")
	}
}
