package marshalling

import (
	"github.com/SENERGY-Platform/platform-connector-lib/marshalling/base"
	_ "github.com/SENERGY-Platform/platform-connector-lib/marshalling/json"
	_ "github.com/SENERGY-Platform/platform-connector-lib/marshalling/plaintext"
	_ "github.com/SENERGY-Platform/platform-connector-lib/marshalling/xml"
)

func Get(key string) (marshaller base.Marshaller, ok bool) {
	return base.Get(key)
}
