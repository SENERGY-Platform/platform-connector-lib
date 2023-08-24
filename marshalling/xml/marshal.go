package xml

import (
	"errors"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/SENERGY-Platform/platform-connector-lib/marshalling/base"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/clbanning/mxj"
	"slices"
	"strings"
)

func (Marshaller) Marshal(in interface{}, variable model.ContentVariable) (out string, err error) {
	in, err = rewriteFieldNamesFromSerializationOptions(in, variable, func(name string, options []string) string {
		if !strings.HasPrefix(name, "-") && slices.Contains(options, models.SerializationOptionXmlAttribute) {
			name = "-" + name
		}
		return name
	})
	if err != nil {
		return "", errors.Join(base.ErrUnableToMarshal, err)
	}
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
