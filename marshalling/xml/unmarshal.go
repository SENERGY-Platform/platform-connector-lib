package xml

import (
	"errors"
	"github.com/clbanning/mxj"
	"mapping/model"
)

func (Marshaller) Unmarshal(in string, variable model.ContentVariable) (out interface{}, err error) {
	temp, err := mxj.NewMapXml([]byte(in), true)
	if err != nil {
		return nil, err
	}
	out, ok := temp[variable.Name]
	if !ok {
		return out, errors.New("root element tag != root variable name")
	}
	return out, nil
}
