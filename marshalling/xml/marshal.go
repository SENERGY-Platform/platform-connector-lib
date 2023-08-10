package xml

import (
	"errors"
	"github.com/SENERGY-Platform/platform-connector-lib/marshalling/base"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/clbanning/mxj"
)

func (Marshaller) Marshal(in interface{}, variable model.ContentVariable) (out string, err error) {
	mv, ok := in.(map[string]interface{})
	if !ok {
		mv = map[string]interface{}{variable.Name: in}
		temp, err := mxj.Map(mv).Xml()
		if err != nil {
			return "", errors.Join(base.ErrUnableToMarshal, err)
		}
		return string(temp), nil
	} else {
		temp, err := mxj.Map(mv).Xml(variable.Name)
		if err != nil {
			return "", errors.Join(base.ErrUnableToMarshal, err)
		}
		return string(temp), nil
	}
}
