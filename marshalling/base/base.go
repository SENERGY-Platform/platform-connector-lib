package base

import (
	"errors"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"sync"
)

type Marshaller interface {
	Marshal(in interface{}, variable model.ContentVariable) (out string, err error)
	Unmarshal(in string, variable model.ContentVariable) (out interface{}, err error)
}

var Marshallers = map[string]Marshaller{}

var mux = sync.Mutex{}

func Register(key string, marshaller Marshaller) {
	mux.Lock()
	defer mux.Unlock()
	Marshallers[key] = marshaller
}

func Get(key string) (marshaller Marshaller, ok bool) {
	marshaller, ok = Marshallers[key]
	return
}

var ErrUnableToMarshal = errors.New("unable to marshal message")
var ErrUnableToUnmarshal = errors.New("unable to unmarshal message")
