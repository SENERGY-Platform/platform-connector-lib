package json

import (
	"encoding/json"
	"github.com/SENERGY-Platform/platform-connector-lib/marshalling/base"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
)

type Marshaller struct {
}

const Format = "json"

func init() {
	base.Register(Format, Marshaller{})
}

func (Marshaller) Marshal(in interface{}, variable model.ContentVariable) (out string, err error) {
	temp, err := json.Marshal(in)
	if err != nil {
		return "", err
	}
	return string(temp), err
}

func (Marshaller) Unmarshal(in string, variable model.ContentVariable) (out interface{}, err error) {
	err = json.Unmarshal([]byte(in), &out)
	return
}
