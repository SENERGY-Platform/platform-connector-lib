package cache

import (
	"errors"
	"github.com/SENERGY-Platform/platform-connector-lib/statistics"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/coocood/freecache"
	"log"
	"time"
)

var L1Expiration = 2           // 2sec
var L1Size = 100 * 1024 * 1024 //100MB
var Debug = false

type Cache struct {
	l1         *freecache.Cache
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
	return &Cache{l1: freecache.NewCache(L1Size), l2: m, statistics: statistics.Void{}}
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
	item.Value, err = this.l1.Get([]byte(key))
	if err != nil && err != freecache.ErrNotFound {
		log.Println("ERROR: in Cache::l1.Get()", err)
	}
	if err != nil {
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
		err := this.l1.Set([]byte(key), temp.Value, L1Expiration)
		if err != nil {
			log.Println("ERROR: in Cache::l1.Set()", err)
		}
		item.Value = temp.Value
	}
	return
}

func (this *Cache) Set(key string, value []byte, expiration int32) {
	err := this.l1.Set([]byte(key), value, L1Expiration)
	if err != nil {
		log.Println("ERROR: in Cache::l1.Set()", err)
	}
	if this.l2 != nil {
		err = this.l2.Set(&memcache.Item{Value: value, Expiration: expiration, Key: key})
		if err != nil {
			log.Println("ERROR: in Cache::l2.Set()", err)
		}
	}
	return
}

func (this *Cache) Remove(key string) {
	this.l1.Del([]byte(key))
	if this.l2 != nil {
		this.l2.Delete(key)
	}
}
