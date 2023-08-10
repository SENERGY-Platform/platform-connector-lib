package json

import (
	"encoding/json"
	"errors"
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
		return "", errors.Join(base.ErrUnableToMarshal, err)
	}
	return string(temp), nil
}

func (Marshaller) Unmarshal(in string, variable model.ContentVariable) (out interface{}, err error) {
	err = json.Unmarshal([]byte(in), &out)
	if err != nil {
		return out, errors.Join(base.ErrUnableToUnmarshal, err)
	}
	return out, nil
}
