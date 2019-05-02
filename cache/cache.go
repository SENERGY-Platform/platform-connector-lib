package cache

import (
	"errors"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/coocood/freecache"
	"log"
)

var L1Expiration = 2          // 2sec
var L1Size = 20 * 1024 * 1024 //20MB
var Debug = false

type Cache struct {
	l1 *freecache.Cache
	l2 *memcache.Client
}

type Item struct {
	Key   string
	Value []byte
}

var ErrNotFound = errors.New("key not found in cache")

func New(memcacheUrl ...string) *Cache {
	return &Cache{l1: freecache.NewCache(L1Size), l2: memcache.New(memcacheUrl...)}
}

func (this *Cache) Get(key string) (item Item, err error) {
	item.Value, err = this.l1.Get([]byte(key))
	if err != nil && err != freecache.ErrNotFound {
		log.Println("ERROR: in Cache::l1.Get()", err)
	}
	if err != nil {
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
	err = this.l2.Set(&memcache.Item{Value: value, Expiration: expiration, Key: key})
	if err != nil {
		log.Println("ERROR: in Cache::l2.Set()", err)
	}
	return
}
