package marshalling

import (
	"errors"
	"github.com/SENERGY-Platform/platform-connector-lib/marshalling/base"
	_ "github.com/SENERGY-Platform/platform-connector-lib/marshalling/json"
	_ "github.com/SENERGY-Platform/platform-connector-lib/marshalling/plaintext"
	_ "github.com/SENERGY-Platform/platform-connector-lib/marshalling/xml"
)

func Get(key string) (marshaller base.Marshaller, ok bool) {
	return base.Get(key)
}

func IsMarshallingErr(err error) bool {
	return errors.Is(err, base.ErrUnableToMarshal) || errors.Is(err, base.ErrUnableToUnmarshal)
}
