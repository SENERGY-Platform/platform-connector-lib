package xml

import (
	"mapping/marshalling/base"
	"mapping/model"
	"reflect"
	"testing"
)

func TestUnmarshalSimpleInt(t *testing.T) {
	value := `<i>24</i>`

	marshaller, ok := base.Get(Format)
	if !ok {
		return
	}

	out, err := marshaller.Unmarshal(value, model.ContentVariable{Name: "i", ValueType: model.Integer})

	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(out, float64(24)) {
		t.Fatal(out)
	}
}

func TestUnmarshalSimpleString(t *testing.T) {
	value := `<s>foobar</s>`

	marshaller, ok := base.Get(Format)
	if !ok {
		return
	}

	out, err := marshaller.Unmarshal(value, model.ContentVariable{Name: "s", ValueType: model.String})

	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(out, "foobar") {
		t.Fatal(out)
	}
}

func TestUnmarshalSimpleMap(t *testing.T) {
	value := `<example attr="attrVal"><body>bodyVal</body></example>`

	marshaller, ok := base.Get(Format)
	if !ok {
		return
	}

	out, err := marshaller.Unmarshal(value, model.ContentVariable{
		Name:      "example",
		ValueType: model.Structure,
		SubContentVariables: []model.ContentVariable{
			{Name: "-attr"},
			{Name: "body"},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(out, map[string]interface{}{"-attr": "attrVal", "body": "bodyVal"}) {
		t.Fatal(out)
	}
}

func TestUnmarshalSimpleList(t *testing.T) {
	value := `<list><element>1</element><element>2</element><element>3</element></list>`

	marshaller, ok := base.Get(Format)
	if !ok {
		return
	}

	out, err := marshaller.Unmarshal(value, model.ContentVariable{
		Name:      "list",
		ValueType: model.List,
		SubContentVariables: []model.ContentVariable{
			{Name: "*", ValueType: model.Integer},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(out, map[string]interface{}{"element": []interface{}{float64(1), float64(2), float64(3)}}) {
		t.Fatal(out)
	}
}

//TODO: list tests
