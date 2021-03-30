package security

import (
	"github.com/SENERGY-Platform/platform-connector-lib/cache"
	"log"
)

func (this *Security) GetCachedUserToken(username string) (token JwtToken, err error) {
	if this.cache != nil {
		token, err = this.getTokenFromCache(username)
		if err == nil {
			return
		}
		if err != cache.ErrNotFound {
			log.Println("ERROR: GetCachedUserToken() ", err)
		}
	}
	token, err = this.ExchangeUserToken(username)
	if err != nil {
		log.Println("ERROR: GetCachedUserToken::GenerateUserToken()", err, username)
		return
	}
	if this.cache != nil {
		this.saveDeviceToCache(username, token)
	}
	return
}

func (this *Security) getTokenFromCache(username string) (token JwtToken, err error) {
	item, err := this.cache.Get("token." + username)
	if err != nil {
		return token, err
	}
	return JwtToken(item.Value), err
}

func (this *Security) saveDeviceToCache(username string, token JwtToken) {
	this.cache.Set("token."+username, []byte(token), this.tokenCacheExpiration)
}
