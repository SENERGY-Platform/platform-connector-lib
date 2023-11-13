package security

import (
	"github.com/SENERGY-Platform/service-commons/pkg/cache"
	"log"
	"time"
)

func (this *Security) GetCachedUserToken(username string) (token JwtToken, err error) {
	if this.cache == nil {
		token, err = this.ExchangeUserToken(username)
		if err != nil {
			log.Println("ERROR: GetCachedUserToken::GenerateUserToken()", err, username)
			return
		}
		return token, nil
	}
	return cache.Use(this.cache, "token."+username, func() (JwtToken, error) {
		return this.ExchangeUserToken(username)
	}, time.Duration(this.tokenCacheExpiration)*time.Second)
}
