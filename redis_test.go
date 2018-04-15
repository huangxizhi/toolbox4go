package toolbox4go

import (
	"testing"
)

func TestSet(t *testing.T) {
	key, val := "name", "redis"
	_, err := GetRedisClient().Set(key, val)
	if err != nil {
		t.Errorf("set err:%v", err)
	}

	rVal, err := GetRedisClient().Get(key)
	if err != nil {
		t.Errorf("get err:%v", err)
	}
	if rVal != val {
		t.Errorf("set and get not the save")
	}

}
