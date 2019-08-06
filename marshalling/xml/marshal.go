package xml

import (
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/clbanning/mxj"
)

func (Marshaller) Marshal(in interface{}, variable model.ContentVariable) (out string, err error) {
	mv, ok := in.(map[string]interface{})
	if !ok {
		mv = map[string]interface{}{variable.Name: in}
		temp, err := mxj.Map(mv).Xml()
		return string(temp), err
	} else {
		temp, err := mxj.Map(mv).Xml(variable.Name)
		return string(temp), err
	}
}
