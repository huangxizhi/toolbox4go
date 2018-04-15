package toolbox4go

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strconv"
	"time"
	"github.com/astaxie/beego/logs"
	"errors"
	//"github.com/astaxie/beego"
	"reflect"
)

var (
	redisIp string
	redisPort string
	redisPwd string
	redisClient *RedisClient
)

type RedisClient struct {
	pool      *redis.Pool
	redisIp   string
	redisPort string
	redisDb   int
}

/*func init() {
	redisIp = beego.AppConfig.String("redis.ip")
	redisPort = beego.AppConfig.String("redis.port")
	redisPwd = beego.AppConfig.String("redis.pwd")

	//初始化redis连接池
	redisClient = NewRedisClient(redisIp, redisPort, 0)
}*/

func GetRedisClient() *RedisClient {
	if redisClient == nil {
		redisClient = NewRedisClient(redisIp, redisPort, 0)
	}
	return redisClient
}

func NewRedisClient(ip, port string, redis_db int) *RedisClient {
	connStr := fmt.Sprintf("%s:%s", ip, port)
	p := &redis.Pool{
		MaxIdle:     3,
		MaxActive:   10000,
		IdleTimeout: 180 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", connStr)
			if err != nil {
				return nil, err
			}
			// 选择db
			if redisPwd != "" {
				logs.Info("redis use password:%s", redisPwd)
				c.Do("auth", redisPwd)
			} else {
				logs.Info("redis not use password")
			}
			c.Do("SELECT", redis_db)
			return c, nil
		},
	}

	c := &RedisClient{
		pool:      p,
		redisIp:   ip,
		redisPort: port,
		redisDb:   redis_db,
	}
	return c
}

func (this *RedisClient) GetConn() redis.Conn {
	rc := this.pool.Get()
	return rc
}

func (this *RedisClient) Set(key, val interface{}) (interface{}, error) {
	c := this.GetConn()
	defer c.Close()
	return c.Do("set", key, val)
}

func (this *RedisClient) Get(key string) (string, error) {
	c := this.GetConn()
	defer c.Close()
	b, err := c.Do("get", key)
	if err != nil {
		return "", err
	}

	if b == nil {
		return "", nil
	}

	if v, ok := b.([]byte); ok {
		return string(v), nil
	} else {
		return "", errors.New("assert wrong type[get]")
	}
}

func (this *RedisClient) Expire(key string, t int)  (interface{}, error){
	c := this.GetConn()
	defer c.Close()
	return c.Do("expire", key, t)
}

func (this *RedisClient) Setnx(key, val interface{}) (int64, error) {
	c := this.GetConn()
	defer c.Close()
	reply, err := c.Do("setnx", key, val)
	if err != nil {
		return -1, err
	}

	if v, ok := reply.(int64); ok {
		return v, nil
	} else {
		return -2, errors.New("assert wrong type[setnx]" + reflect.TypeOf(v).String())
	}
}

func (this *RedisClient) Ttl(key string) (int64, error) {
	c := this.GetConn()
	defer c.Close()
	reply, err := c.Do("ttl", key)
	if err != nil {
		return -1, err
	}

	if v, ok := reply.(int64); ok {
		return v, nil
	} else {
		return -2, errors.New("assert wrong type[setnx]" + reflect.TypeOf(v).String())
	}
}

func (this *RedisClient) SetWithExpire(key, val interface{}, t int) (interface{}, error) {
	c := this.GetConn()
	defer c.Close()
	_, err := c.Do("set", key, val)
	if err != nil {
		return nil, err
	}

	return c.Do("expire", key, t)
}



func (this *RedisClient) Sadd(key string, val string) (interface{}, error) {
	c := this.GetConn()
	defer c.Close()
	v, err := c.Do("sadd", key, val)
	return v, err
}

func (this *RedisClient) Del(key string) (int64, error) {
	c := this.GetConn()
	defer c.Close()
	_v, err := c.Do("del", key)
	if err != nil {
		return -1, err
	}

	if v, ok := _v.(int64); ok {
		return v, nil
	} else {
		return -2, errors.New("wrong response type assert:%v" + reflect.TypeOf(v).String())
	}
}


func (this *RedisClient) Exist(key string) (bool, error) {
	c := this.GetConn()
	defer c.Close()
	_v, err := c.Do("exists", key)
	if err != nil {
		return true, err
	}

	if v, ok := _v.(int64); ok {
		if int64(v) > 0 {
			return true, nil
		} else {
			return false, nil
		}
	}
	return false, errors.New("assert wrong type, try reflect.TypeOf(_v).String()")
}

func (this *RedisClient) HGetAll(key string) (map[string]string, error) {
	c := this.GetConn()
	defer c.Close()
	_v, err := c.Do("hgetall", key)
	if err != nil {
		//logs.Error("hgetall v:%v, err:%v", _v, err)
		return nil, err
	}

	//logs.Warn("hgetall:%v", _v)
	var m map[string]string = map[string]string{}
	if intf, ok := _v.([]interface{}); ok {
		var (
			key string
			val string
			idx int = 0
		)
		for _, _b := range intf {
			if b, ok := _b.([]byte); ok {
				if idx == 0 {
					key = string(b)
					idx = 1
				} else if idx == 1 {
					val = string(b)
					m[key] = val
					idx = 0
				}
			}
		}
	}

	return m, nil
}

func (this *RedisClient) Hdel(key string, field string) (interface{} ,error) {
	c := this.GetConn()
	defer c.Close()
	v, err := c.Do("hdel", key, field)
	return v, err
}

func (this *RedisClient) Hset(key string, field string, val interface{}) (interface{} ,error) {
	c := this.GetConn()
	defer c.Close()
	v, err := c.Do("hset", key, field, val)
	return v, err
}

func (this *RedisClient) Hget(key string, field string) (string ,error) {
	c := this.GetConn()
	defer c.Close()
	_v, err := c.Do("hget", key, field)
	if err != nil {
		return "", err
	}

	if _v == nil {
		return "", nil
	}

	if v, ok := _v.([]byte); ok {
		return string(v), nil
	} else {
		return "", errors.New("assert wrong type[hget]:" + reflect.TypeOf(_v).String())
	}
}

func (this *RedisClient) Srem(key string, val string) (interface{}, error) {
	c := this.GetConn()
	defer c.Close()
	v, err := c.Do("srem", key, val)
	return v, err
}

func (this *RedisClient) Rpush(key string, val interface{}) (interface{}, error) {
	c := this.GetConn()
	defer c.Close()
	return c.Do("rpush", key, val)
}

//删除list中的一个元素
func (this *RedisClient) LRem(key string, val string) (interface{}, error) {
	c := this.GetConn()
	defer c.Close()
	return c.Do("lrem", key, 0, val)	//删除所有的值
}

func (this *RedisClient) LLen(key string) (int64, error) {
	c := this.GetConn()
	defer c.Close()
	_v, err := c.Do("llen", key)
	if err != nil {
		return -1, err
	}

	if v, ok := _v.(int64); ok {
		return v, nil
	} else {
		return -2, errors.New("wrong response type assert")
	}
}

func (this *RedisClient) LRange(key string, start, end int) ([]string, error) {
	c := this.GetConn()
	defer c.Close()
	_v, err := c.Do("lrange", key, start, end)
	if err != nil {
		return []string{}, err
	}

	result := []string{}
	if l, ok := _v.([]interface{}); ok {
		for _, item := range l {
			if s, ok := item.([]byte); ok {
				result = append(result, string(s))
			}
		}
	}
	return result, err
}

func (this *RedisClient) LPop(key string) (string, error) {
	c := this.GetConn()
	defer c.Close()
	_v, err := c.Do("lpop", key)
	if err != nil {
		return "", err
	}


	if v, ok := _v.([]byte); ok {
		return string(v), nil
	} else {
		return "", errors.New("assert wrong type[lpop]")
	}
}


func (this *RedisClient) SaddBatch(inputCh chan string, key string) (finishCh chan bool) {
	finishCh = make(chan bool, 1)
	go func() {
		c := this.GetConn()
		defer c.Close()
		for {
			if v, ok := <-inputCh; ok {
				_, err := c.Do("sadd", key, v)
				if err != nil {
					logs.Error("batch sadd:%v", err)
				}
			} else {
				finishCh <- true
				break
			}
		}
	}()

	return
}

func (this *RedisClient) Smembers(key string) ([]string, error) {
	c := this.GetConn()
	defer c.Close()
	err := c.Send("smembers", key)
	if err != nil {
		panic(err)
	}
	c.Flush()
	reply, err := redis.MultiBulk(c.Receive())
	if err != nil {
		panic(err)
	}

	r := make([]string, 0, len(reply))
	for _, x := range reply {
		var v, ok = x.([]byte)
		if ok {
			r = append(r, string(v))
		}
	}

	return r, err
}

func (this *RedisClient) Sismember(key, val string) bool {
	c := this.GetConn()
	defer c.Close()

	v, err := c.Do("sismember", key, val)
	if err != nil {
		logs.Error("redis sismember cmd execute error:%v", err)
		panic(err)
	}

	n, err := strconv.Atoi(fmt.Sprintf("%v", v))
	if err == nil {
		if n > 0 {
			return true
		} else {
			return false
		}
	}

	return false
}

//keepExist：true如果存在就留下，否则过滤掉
func (this *RedisClient) FilterExistOrNot(key string, valList []string, keepExist bool) (keeped []string, err error) {
	if len(valList) <= 0 {
		return valList, err
	}
	c := this.GetConn()
	defer c.Close()

	keeped = make([]string, 0, len(valList))
	for _, v := range valList {
		r, err := c.Do("sismember", key, v)
		if err != nil {
			logs.Error("redis/FilterExistOrNot sismember exec err:%v", err)
			return keeped, err
		}
		n, err := strconv.Atoi(fmt.Sprintf("%v", r))
		if err != nil {
			logs.Error("redis/FilterExistOrNot Atoi err:%v", err)
			return keeped, err
		}
		if (n > 0 && keepExist) || (n <= 0 && !keepExist) {
			keeped = append(keeped, v)
		}
	}

	return
}

// --------  有序列表  --------
func (this *RedisClient) Zadd(key string, field string, score int64) (interface{}, error){
	c := this.GetConn()
	defer c.Close()
	//return c.Do("zadd", key, "CH", score, field)
	return c.Do("zadd", key, score, field)
}

// 大于某个分数的所有字段的list
func (this *RedisClient) ZRevRangeByScore(key string, low, high string) ([]string, error){
	c := this.GetConn()
	defer c.Close()

	reply, err := c.Do("zrevrangebyscore", key, high, low)
	if err != nil {
		return []string{}, err
	}

	result := []string{}
	if l, ok := reply.([]interface{}); ok {
		for _, item := range l {
			if s, ok := item.([]byte); ok {
				result = append(result, string(s))
			}
		}
	}

	return result, nil
}

func (this *RedisClient) ZRangeByScore(key string, low, high interface{}) ([]string, error){
	c := this.GetConn()
	defer c.Close()

	reply, err := c.Do("zrangebyscore", key, low, high)
	if err != nil {
		return []string{}, err
	}

	result := []string{}
	if l, ok := reply.([]interface{}); ok {
		for _, item := range l {
			if s, ok := item.([]byte); ok {
				result = append(result, string(s))
			}
		}
	}

	return result, nil
}

func (this *RedisClient) ZRem(key, field string) (interface{}, error) {
	c := this.GetConn()
	defer c.Close()

	return c.Do("zrem", key, field)
}

//获取某个field的score （可以用来判断某个filed是否存在）=> 返回的score是字符串
func (this *RedisClient) Zscore(key, field string) (string, error) {
	c := this.GetConn()
	defer c.Close()
	reply, err := c.Do("zscore", key, field)
	if err != nil {
		return "", err
	} else {
		if reply == nil {
			return "", nil
		}

		if v, ok := reply.([]byte); ok {
			return string(v), nil
		} else {
			return "", errors.New("assert wrong type[Zscore]")
		}
	}
}


