package xml

import (
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/SENERGY-Platform/platform-connector-lib/marshalling/base"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/clbanning/mxj"
	"reflect"
	"slices"
	"strings"
)

func (Marshaller) Unmarshal(in string, variable model.ContentVariable) (out interface{}, err error) {
	temp, err := mxj.NewMapXml([]byte(in), true)
	if err != nil {
		return nil, errors.Join(base.ErrUnableToUnmarshal, err)
	}
	out, ok := temp[variable.Name]
	if !ok {
		return out, errors.Join(base.ErrUnableToUnmarshal, errors.New("root element tag != root variable name"))
	}
	//match variable names to mxj output, to enable rewriteFieldNamesFromSerializationOptions() to find sub variables
	variable, err = rewriteContentVariableWithSerializationOptions(variable)
	if err != nil {
		return nil, errors.Join(base.ErrUnableToUnmarshal, err)
	}
	out, err = rewriteFieldNamesFromSerializationOptions(out, variable, func(name string, options []string) string {
		if strings.HasPrefix(name, "-") && slices.Contains(options, models.SerializationOptionXmlAttribute) {
			name = strings.TrimPrefix(name, "-")
		}
		return name
	})
	if err != nil {
		return nil, errors.Join(base.ErrUnableToUnmarshal, err)
	}
	return out, nil
}

func rewriteFieldNamesFromSerializationOptions(value interface{}, variable model.ContentVariable, f func(name string, options []string) string) (interface{}, error) {
	//ensure generic map
	if reflect.ValueOf(value).Kind() == reflect.Map {
		switch value.(type) {
		case map[string]interface{}:
		default:
			val := make(map[string]interface{})
			vv := reflect.ValueOf(value)
			keys := vv.MapKeys()
			for _, k := range keys {
				val[fmt.Sprint(k)] = vv.MapIndex(k).Interface()
			}
			value = val
		}
	}

	var err error

	switch v := value.(type) {
	case map[string]interface{}:
		result := map[string]interface{}{}
		for fieldname, field := range v {
			subVar, found := getSubVar(variable, fieldname)
			if found {
				field, err = rewriteFieldNamesFromSerializationOptions(field, subVar, f)
				if err != nil {
					return result, err
				}
				fieldname = f(fieldname, subVar.SerializationOptions)
			}
			result[fieldname] = field
		}
		return result, nil
	default:
		return v, nil
	}
}

func getSubVar(variable model.ContentVariable, fieldname string) (result model.ContentVariable, found bool) {
	for _, sub := range variable.SubContentVariables {
		if fieldname == sub.Name {
			return sub, true
		}
	}
	return result, false
}

func rewriteContentVariableWithSerializationOptions(variable model.ContentVariable) (model.ContentVariable, error) {
	if slices.Contains(variable.SerializationOptions, models.SerializationOptionXmlAttribute) && !strings.HasPrefix(variable.Name, "-") {
		variable.Name = "-" + variable.Name
	}
	subvariables := []models.ContentVariable{}
	for _, sub := range variable.SubContentVariables {
		temp, err := rewriteContentVariableWithSerializationOptions(sub)
		if err != nil {
			return variable, err
		}
		subvariables = append(subvariables, temp)
	}
	variable.SubContentVariables = subvariables
	return variable, nil
}
