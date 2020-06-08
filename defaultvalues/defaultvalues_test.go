/*
 * Copyright 2020 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package defaultvalues

import (
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"testing"
)

func TestFillComplex(t *testing.T) {
	service := model.Service{
		Outputs: []model.Content{{ContentVariable: model.ContentVariable{
			Name: "complex",
			Type: model.Structure,
			SubContentVariables: []model.ContentVariable{
				{Name: "str", Type: model.String, Value: "str"},
				{Name: "int", Type: model.Integer, Value: 42},
				{Name: "float", Type: model.Float, Value: 2.4},
				{Name: "bool", Type: model.Boolean, Value: true},
				{Name: "map", Type: model.Structure, SubContentVariables: []model.ContentVariable{
					{Name: "str2", Type: model.String, Value: "str2"},
					{Name: "str3", Type: model.String, Value: "str3"},
				}},
				{Name: "list", Type: model.List, SubContentVariables: []model.ContentVariable{
					{Name: "*", Type: model.String, Value: []interface{}{"a", "b"}},
				}},
			},
		}}},
	}
	t.Run("str", func(t *testing.T) {
		msg := getCorrectMsg()
		complexMsg, ok := msg["complex"]
		if !ok {
			t.Error("unexpected message")
		}
		complexMap, ok := complexMsg.(map[string]interface{})
		if !ok {
			t.Error("unexpected message")
		}
		delete(complexMap, "str")
		err := FillDefaultValues(&msg, service)
		if err != nil {
			t.Error(err)
		}
		if !isMsgOk(msg) {
			t.Error("Could not fill string")
		}
	})
	t.Run("int", func(t *testing.T) {
		msg := getCorrectMsg()
		complexMsg, ok := msg["complex"]
		if !ok {
			t.Error("unexpected message")
		}
		complexMap, ok := complexMsg.(map[string]interface{})
		if !ok {
			t.Error("unexpected message")
		}
		delete(complexMap, "int")
		err := FillDefaultValues(&msg, service)
		if err != nil {
			t.Error(err)
		}
		if !isMsgOk(msg) {
			t.Error("Could not fill int")
		}
	})
	t.Run("float", func(t *testing.T) {
		msg := getCorrectMsg()
		complexMsg, ok := msg["complex"]
		if !ok {
			t.Error("unexpected message")
		}
		complexMap, ok := complexMsg.(map[string]interface{})
		if !ok {
			t.Error("unexpected message")
		}
		delete(complexMap, "float")
		err := FillDefaultValues(&msg, service)
		if err != nil {
			t.Error(err)
		}
		if !isMsgOk(msg) {
			t.Error("Could not fill float")
		}
	})
	t.Run("bool", func(t *testing.T) {
		msg := getCorrectMsg()
		complexMsg, ok := msg["complex"]
		if !ok {
			t.Error("unexpected message")
		}
		complexMap, ok := complexMsg.(map[string]interface{})
		if !ok {
			t.Error("unexpected message")
		}
		delete(complexMap, "bool")
		err := FillDefaultValues(&msg, service)
		if err != nil {
			t.Error(err)
		}
		if !isMsgOk(msg) {
			t.Error("Could not fill string")
		}
	})
	t.Run("map", func(t *testing.T) {
		msg := getCorrectMsg()
		complexMsg, ok := msg["complex"]
		if !ok {
			t.Error("unexpected message")
		}
		complexMap, ok := complexMsg.(map[string]interface{})
		if !ok {
			t.Error("unexpected message")
		}
		delete(complexMap, "map")
		err := FillDefaultValues(&msg, service)
		if err != nil {
			t.Error(err)
		}
		if !isBaseValuesOk(msg) || !isListValuesOk(msg) { //don't expect map to be filled here
			t.Error("Could not fill map")
		}
	})
	t.Run("map-partial", func(t *testing.T) {
		msg := getCorrectMsg()
		complexMsg, ok := msg["complex"]
		if !ok {
			t.Error("unexpected message")
		}
		complexMap, ok := complexMsg.(map[string]interface{})
		if !ok {
			t.Error("unexpected message")
		}
		complexMap["map"] = map[string]interface{}{"str2": "str2"}
		err := FillDefaultValues(&msg, service)
		if err != nil {
			t.Error(err)
		}
		if !isMsgOk(msg) {
			t.Error("Could not fill map")
		}
	})
	t.Run("list", func(t *testing.T) {
		msg := getCorrectMsg()
		complexMsg, ok := msg["complex"]
		if !ok {
			t.Error("unexpected message")
		}
		complexMap, ok := complexMsg.(map[string]interface{})
		if !ok {
			t.Error("unexpected message")
		}
		delete(complexMap, "list")
		err := FillDefaultValues(&msg, service)
		if err != nil {
			t.Error(err)
		}
		if !isBaseValuesOk(msg) || !isMapValuesOk(msg) { //don't expect list to be filled here
			t.Error("Could not fill list")
		}
	})
	t.Run("list-partial", func(t *testing.T) {
		msg := getCorrectMsg()
		complexMsg, ok := msg["complex"]
		if !ok {
			t.Error("unexpected message")
		}
		complexMap, ok := complexMsg.(map[string]interface{})
		if !ok {
			t.Error("unexpected message")
		}
		complexMap["list"] = []interface{}{"a"}
		err := FillDefaultValues(&msg, service)
		if err != nil {
			t.Error(err)
		}
		if isMsgOk(msg) { //should not overwrite value
			t.Error("Filled list with extra values")
		}
	})
	t.Run("all-nil", func(t *testing.T) {
		msg := getCorrectMsg()
		msg["complex"] = nil
		err := FillDefaultValues(&msg, service)
		if err != nil {
			t.Error(err)
		}
		if isMsgOk(msg) { //don't expect anything to be filled here
			t.Error("Could not fill all missing")
		}
	})
	t.Run("all-no-key", func(t *testing.T) {
		msg := getCorrectMsg()
		delete(msg, "complex")
		err := FillDefaultValues(&msg, service)
		if err != nil {
			t.Error(err)
		}
		if isMsgOk(msg) { //message misses completely, can't fill values
			t.Error("Could not fill all missing")
		}
	})

}

func isMsgOk(msg map[string]interface{}) bool {
	complexMsg, ok := msg["complex"]
	if !ok {
		return false
	}
	complexMap, ok := complexMsg.(map[string]interface{})
	if !ok {
		return false
	}

	actual, ok := complexMap["str"]
	if !ok || actual != "str" {
		return false
	}
	actual, ok = complexMap["int"]
	if !ok || actual != 42 {
		return false
	}
	actual, ok = complexMap["float"]
	if !ok || actual != 2.4 {
		return false
	}
	actual, ok = complexMap["bool"]
	if !ok || actual != true {
		return false
	}

	actual, ok = complexMap["map"]
	if !ok {
		return false
	}
	m, ok := actual.(map[string]interface{})
	if !ok {
		return false
	}
	actual, ok = m["str2"]
	if !ok || actual != "str2" {
		return false
	}
	actual, ok = m["str3"]
	if !ok || actual != "str3" {
		return false
	}

	actual, ok = complexMap["list"]
	if !ok {
		return false
	}
	l, ok := actual.([]interface{})
	if !ok || len(l) != 2 || l[0] != "a" || l[1] != "b" {
		return false
	}

	return true
}

func isBaseValuesOk(msg map[string]interface{}) bool {
	complexMsg, ok := msg["complex"]
	if !ok {
		return false
	}
	complexMap, ok := complexMsg.(map[string]interface{})
	if !ok {
		return false
	}

	actual, ok := complexMap["str"]
	if !ok || actual != "str" {
		return false
	}
	actual, ok = complexMap["int"]
	if !ok || actual != 42 {
		return false
	}
	actual, ok = complexMap["float"]
	if !ok || actual != 2.4 {
		return false
	}
	actual, ok = complexMap["bool"]
	if !ok || actual != true {
		return false
	}

	return true
}

func isMapValuesOk(msg map[string]interface{}) bool {
	complexMsg, ok := msg["complex"]
	if !ok {
		return false
	}
	complexMap, ok := complexMsg.(map[string]interface{})
	if !ok {
		return false
	}

	actual, ok := complexMap["map"]
	if !ok {
		return false
	}
	m, ok := actual.(map[string]interface{})
	if !ok {
		return false
	}
	actual, ok = m["str2"]
	if !ok || actual != "str2" {
		return false
	}
	actual, ok = m["str3"]
	if !ok || actual != "str3" {
		return false
	}

	return true
}

func isListValuesOk(msg map[string]interface{}) bool {
	complexMsg, ok := msg["complex"]
	if !ok {
		return false
	}
	complexMap, ok := complexMsg.(map[string]interface{})
	if !ok {
		return false
	}

	actual, ok := complexMap["list"]
	if !ok {
		return false
	}
	l, ok := actual.([]interface{})
	if !ok || len(l) != 2 || l[0] != "a" || l[1] != "b" {
		return false
	}

	return true
}

func getCorrectMsg() map[string]interface{} {
	return map[string]interface{}{
		"complex": map[string]interface{}{
			"str":   "str",
			"int":   42,
			"float": 2.4,
			"bool":  true,
			"map":   map[string]interface{}{"str2": "str2", "str3": "str3"},
			"list":  []interface{}{"a", "b"},
		},
	}
}
