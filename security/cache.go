package security

import (
	"errors"
	"github.com/SENERGY-Platform/service-commons/pkg/cache"
	"log"
	"time"
)

func (this *Security) GetCachedUserToken(username string, remoteAddr string) (token JwtToken, err error) {
	if this.cache == nil {
		token, err = this.ExchangeUserToken(username, remoteAddr)
		if err != nil {
			log.Println("ERROR: GetCachedUserToken::GenerateUserToken()", err, username)
			return
		}
		return token, nil
	}
	return cache.Use(this.cache, "token."+username, func() (JwtToken, error) {
		return this.ExchangeUserToken(username, remoteAddr)
	}, func(token JwtToken) error {
		if token == "" || token == "Bearer " {
			return errors.New("missing token")
		}
		return nil
	}, time.Duration(this.tokenCacheExpiration)*time.Second)
}
