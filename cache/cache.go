package cache

import (
	"errors"
	"github.com/SENERGY-Platform/platform-connector-lib/statistics"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/patrickmn/go-cache"
	"log"
	"time"
)

var Debug = false

type Cache struct {
	l1         *cache.Cache
	l2         *memcache.Client
	statistics statistics.Interface
}

type Item struct {
	Key   string
	Value []byte
}

var ErrNotFound = errors.New("key not found in cache")

func New(maxIdleConns int, timeout time.Duration, memcacheUrl ...string) *Cache {
	var m *memcache.Client
	if len(memcacheUrl) != 0 {
		m = memcache.New(memcacheUrl...)
		m.MaxIdleConns = maxIdleConns
		m.Timeout = timeout
	}
	return &Cache{l1: cache.New(10*time.Second, 30*time.Second), l2: m, statistics: statistics.Void{}}
}

func (this *Cache) SetStatisticsCollector(collector statistics.Interface) *Cache {
	this.statistics = collector
	return this
}

func (this *Cache) Get(key string) (item Item, err error) {
	start := time.Now()
	defer this.statistics.CacheRead(time.Since(start))
	defer func() {
		if err != nil {
			this.statistics.CacheMiss()
		}
	}()
	l1Value, ok := this.l1.Get(key)
	if ok {
		item.Value, ok = l1Value.([]byte)
		return
	}
	if this.l2 == nil {
		err = ErrNotFound
		return
	}
	if Debug {
		log.Println("DEBUG: use l2 cache", key, err)
	}
	var temp *memcache.Item
	temp, err = this.l2.Get(key)
	if err == memcache.ErrCacheMiss {
		err = ErrNotFound
		return
	}
	if err != nil {
		return
	}
	this.l1.Set(key, temp.Value, time.Duration(temp.Expiration)*time.Second)
	item.Value = temp.Value
	return
}

func (this *Cache) Set(key string, value []byte, expiration int32) {
	this.l1.Set(key, value, time.Duration(expiration)*time.Second)
	if this.l2 != nil {
		err := this.l2.Set(&memcache.Item{Value: value, Expiration: expiration, Key: key})
		if err != nil {
			log.Println("ERROR: in Cache::l2.Set()", err)
		}
	}
	return
}

func (this *Cache) Remove(key string) {
	this.l1.Delete(key)
	if this.l2 != nil {
		this.l2.Delete(key)
	}
}
