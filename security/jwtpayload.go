package security

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"reflect"
	"strings"
)

type JwtPayload struct {
	UserId         string                 `json:"sub"`
	ResourceAccess map[string]JwtResource `json:"resource_access"`
	RealmAccess    JwtResource            `json:"realm_access"`
}

type JwtResource struct {
	Roles []string `json:"roles"`
}

func (this JwtToken) GetPayload() (result JwtPayload, err error) {
	err = GetJWTPayload(string(this), &result)
	return
}

func GetJWTPayload(auth string, results ...interface{}) (err error) {
	authParts := strings.Split(auth, " ")
	if len(authParts) != 2 {
		return errors.New("expect auth string format like '<type> <token>'")
	}
	tokenString := authParts[1]
	tokenParts := strings.Split(tokenString, ".")
	if len(tokenParts) != 3 {
		return errors.New("expect token string format like '<head>.<payload>.<sig>'")
	}
	payloadSegment := tokenParts[1]
	err = decodeJWTSegment(payloadSegment, results...)
	return
}

// Decode JWT specific base64url encoding with padding stripped
func decodeJWTSegment(seg string, results ...interface{}) error {
	if l := len(seg) % 4; l > 0 {
		seg += strings.Repeat("=", 4-l)
	}

	b, err := base64.URLEncoding.DecodeString(seg)
	if err != nil {
		log.Println("error while base64.URLEncoding.DecodeString()", err, seg)
		return err
	}

	for _, result := range results {
		err = json.Unmarshal(b, result)
		if err != nil {
			log.Println("error while json.Unmarshal()", err, reflect.TypeOf(result).Kind().String(), string(b))
			return err
		}
	}

	return nil
}
