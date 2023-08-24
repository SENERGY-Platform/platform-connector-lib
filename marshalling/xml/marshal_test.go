package xml

import (
	"fmt"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/SENERGY-Platform/platform-connector-lib/marshalling/base"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
)

func ExampleMarshalPrimitiveInt() {
	value := 24
	marshaller, ok := base.Get(Format)
	if !ok {
		return
	}

	fmt.Println(marshaller.Marshal(value, model.ContentVariable{
		Name: "int",
	}))

	// Output:
	//<int>24</int> <nil>
}

func ExampleMarshalPrimitiveFloat() {
	value := 2.4
	marshaller, ok := base.Get(Format)
	if !ok {
		return
	}

	fmt.Println(marshaller.Marshal(value, model.ContentVariable{
		Name: "f",
	}))

	// Output:
	//<f>2.4</f> <nil>
}

func ExampleMarshalPrimitiveString() {
	value := "foo"
	marshaller, ok := base.Get(Format)
	if !ok {
		return
	}

	fmt.Println(marshaller.Marshal(value, model.ContentVariable{
		Name: "str",
	}))

	// Output:
	//<str>foo</str> <nil>
}

func ExampleMarshalPrimitiveBool() {
	value := true
	marshaller, ok := base.Get(Format)
	if !ok {
		return
	}

	fmt.Println(marshaller.Marshal(value, model.ContentVariable{
		Name: "b",
	}))

	// Output:
	//<b>true</b> <nil>
}

func ExampleMarshal() {
	value := map[string]interface{}{"-attr": "attrVal", "body": "bodyVal"}
	marshaller, ok := base.Get(Format)
	if !ok {
		return
	}

	fmt.Println(marshaller.Marshal(value, model.ContentVariable{
		Name: "example",
		Type: model.Structure,
		SubContentVariables: []model.ContentVariable{
			{Name: "-attr"},
			{Name: "body"},
		},
	}))

	// Output:
	//<example attr="attrVal"><body>bodyVal</body></example> <nil>
}

func ExampleMarshalSerializationOptionXmlAttribute() {
	value := map[string]interface{}{"attr": "attrVal", "body": "bodyVal"}
	marshaller, ok := base.Get(Format)
	if !ok {
		return
	}

	fmt.Println(marshaller.Marshal(value, model.ContentVariable{
		Name: "example",
		Type: model.Structure,
		SubContentVariables: []model.ContentVariable{
			{Name: "attr", SerializationOptions: []string{models.SerializationOptionXmlAttribute}},
			{Name: "body"},
		},
	}))

	// Output:
	//<example attr="attrVal"><body>bodyVal</body></example> <nil>
}

//TODO: lists
