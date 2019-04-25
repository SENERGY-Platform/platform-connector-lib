package security

import (
	"github.com/bradfitz/gomemcache/memcache"
	"log"
)

func (this *Security) GetCachedUserToken(username string) (token JwtToken, err error) {
	if this.memcached != nil {
		token, err = this.getTokenFromCache(username)
		if err == nil {
			return
		}
		if err != memcache.ErrCacheMiss {
			log.Println("ERROR: GetCachedUserToken() ", err)
		}
	}
	token, err = this.GenerateUserToken(username)
	if err != nil {
		log.Println("ERROR: GetCachedUserToken::GenerateUserToken()", err, username)
		return
	}
	if this.memcached != nil {
		this.saveDeviceToCache(username, token)
	}
	return
}

func (this *Security) getTokenFromCache(username string) (token JwtToken, err error) {
	item, err := this.memcached.Get("token." + username)
	if err != nil {
		return token, err
	}
	return JwtToken(item.Value), err
}

func (this *Security) saveDeviceToCache(username string, token JwtToken) {
	_ = this.memcached.Set(&memcache.Item{Key: "token." + username, Value: []byte(token), Expiration: this.tokenCacheExpiration})
}
