package marshalling

import "github.com/SENERGY-Platform/platform-connector-lib/marshalling/base"

func Get(key string) (marshaller base.Marshaller, ok bool) {
	return base.Get(key)
}
