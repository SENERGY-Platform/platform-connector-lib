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

package msgvalidation

import (
	"errors"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"testing"
)

func TestValidateSimpleString(t *testing.T) {
	service := model.Service{
		Outputs: []model.Content{{ContentVariable: model.ContentVariable{
			Name: "simple",
			Type: model.String,
		}}},
	}
	t.Run("matching", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"simple": "abc"})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, nil, nil, nil})
	})
	t.Run("added", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"simple": "abc", "add": "foo"})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, ErrUnexpectedField, nil, ErrUnexpectedField})
	})
	t.Run("removed", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, nil, ErrMissingField, ErrMissingField})
	})
	t.Run("renamed", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"add": "foo"})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF},
			[]error{nil, ErrUnexpectedField, ErrMissingField})
		checkExpectedErrorOptions(t, errFF, ErrUnexpectedField, ErrMissingField)
	})

	t.Run("changedType", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"simple": 13})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{ErrUnexpectedType, ErrUnexpectedType, ErrUnexpectedType, ErrUnexpectedType})
	})
}

func TestValidateSimpleInt(t *testing.T) {
	service := model.Service{
		Outputs: []model.Content{{ContentVariable: model.ContentVariable{
			Name: "simple",
			Type: model.Integer,
		}}},
	}
	t.Run("matching", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"simple": 42})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, nil, nil, nil})
	})
	t.Run("added", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"simple": 42, "add": "foo"})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, ErrUnexpectedField, nil, ErrUnexpectedField})
	})
	t.Run("removed", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, nil, ErrMissingField, ErrMissingField})
	})
	t.Run("renamed", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"add": "foo"})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF},
			[]error{nil, ErrUnexpectedField, ErrMissingField})
		checkExpectedErrorOptions(t, errFF, ErrUnexpectedField, ErrMissingField)
	})

	t.Run("changedType", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"simple": "foo"})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{ErrUnexpectedType, ErrUnexpectedType, ErrUnexpectedType, ErrUnexpectedType})
	})
}

func TestValidateComplex(t *testing.T) {
	service := model.Service{
		Outputs: []model.Content{{ContentVariable: model.ContentVariable{
			Name: "complex",
			Type: model.Structure,
			SubContentVariables: []model.ContentVariable{
				{Name: "str", Type: model.String},
				{Name: "int", Type: model.Integer},
				{Name: "float", Type: model.Float},
				{Name: "bool", Type: model.Boolean},
			},
		}}},
	}
	t.Run("matching", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"complex": map[string]interface{}{"str": "str", "int": 42, "float": 2.4, "bool": true}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, nil, nil, nil})
	})
	t.Run("added", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"complex": map[string]interface{}{"str": "str", "int": 42, "float": 2.4, "bool": true, "foo": "bar"}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, ErrUnexpectedField, nil, ErrUnexpectedField})
	})
	t.Run("removed", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"complex": map[string]interface{}{"str": "str", "int": 42, "float": 2.4}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, nil, ErrMissingField, ErrMissingField})
	})
	t.Run("renamed", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"complex": map[string]interface{}{"str": "str", "int": 42, "float": 2.4, "boolean": true}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF},
			[]error{nil, ErrUnexpectedField, ErrMissingField})
		checkExpectedErrorOptions(t, errFF, ErrUnexpectedField, ErrMissingField)
	})
	t.Run("changedType", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"complex": map[string]interface{}{"str": 13, "int": 42, "float": 2.4, "bool": true}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{ErrUnexpectedType, ErrUnexpectedType, ErrUnexpectedType, ErrUnexpectedType})
	})
	t.Run("int float cast", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"complex": map[string]interface{}{"str": "str", "int": 4.2, "float": 24, "bool": true}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, nil, nil, nil})
	})
}

func TestValidateArray(t *testing.T) {
	service := model.Service{
		Outputs: []model.Content{{ContentVariable: model.ContentVariable{
			Name: "list",
			Type: model.List,
			SubContentVariables: []model.ContentVariable{
				{Name: "str", Type: model.String},
				{Name: "int", Type: model.Integer},
				{Name: "float", Type: model.Float},
				{Name: "bool", Type: model.Boolean},
			},
		}}},
	}
	t.Run("matching", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"list": []interface{}{"str", 42, 2.4, true}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, nil, nil, nil})
	})
	t.Run("added", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"list": []interface{}{"str", 42, 2.4, true, "nope"}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, ErrUnexpectedField, nil, ErrUnexpectedField})
	})
	t.Run("removed", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"list": []interface{}{"str", 42, 2.4}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, nil, ErrMissingField, ErrMissingField})
	})
	t.Run("changedType", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"list": []interface{}{13, "str", 2.4, true}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{ErrUnexpectedType, ErrUnexpectedType, ErrUnexpectedType, ErrUnexpectedType})
	})
}

func TestValidateVarLenList(t *testing.T) {
	service := model.Service{
		Outputs: []model.Content{{ContentVariable: model.ContentVariable{
			Name: "list",
			Type: model.List,
			SubContentVariables: []model.ContentVariable{
				{Name: "*", Type: model.String},
			},
		}}},
	}
	t.Run("matching0", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"list": []interface{}{}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, nil, nil, nil})
	})
	t.Run("matching1", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"list": []interface{}{"a"}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, nil, nil, nil})
	})
	t.Run("matching2", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"list": []interface{}{"a", "b"}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, nil, nil, nil})
	})
	t.Run("matching3", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"list": []interface{}{"a", "b", "c"}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, nil, nil, nil})
	})
	t.Run("changedType1", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"list": []interface{}{"a", 13}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{ErrUnexpectedType, ErrUnexpectedType, ErrUnexpectedType, ErrUnexpectedType})
	})
	t.Run("changedType2", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"list": []interface{}{13}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{ErrUnexpectedType, ErrUnexpectedType, ErrUnexpectedType, ErrUnexpectedType})
	})
	t.Run("changedType3", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"list": []interface{}{13, "a"}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{ErrUnexpectedType, ErrUnexpectedType, ErrUnexpectedType, ErrUnexpectedType})
	})
}

func TestValidateMap(t *testing.T) {
	service := model.Service{
		Outputs: []model.Content{{ContentVariable: model.ContentVariable{
			Name: "map",
			Type: model.Structure,
			SubContentVariables: []model.ContentVariable{
				{Name: "*", Type: model.String},
			},
		}}},
	}
	t.Run("matching0", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"map": map[string]interface{}{}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, nil, nil, nil})
	})
	t.Run("matching1", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"map": map[string]interface{}{"a": "1"}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, nil, nil, nil})
	})
	t.Run("matching2", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"map": map[string]interface{}{"a": "1", "b": "2"}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, nil, nil, nil})
	})
	t.Run("changedType", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"map": map[string]interface{}{"a": 13}})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{ErrUnexpectedType, ErrUnexpectedType, ErrUnexpectedType, ErrUnexpectedType})
	})
}

func TestValidateSegments(t *testing.T) {
	service := model.Service{
		Outputs: []model.Content{
			{ContentVariable: model.ContentVariable{
				Name: "payload",
				Type: model.String,
			}},
			{ContentVariable: model.ContentVariable{
				Name: "code",
				Type: model.Integer,
			}},
		},
	}
	t.Run("matching", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"payload": "abc", "code": 200})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, nil, nil, nil})
	})
	t.Run("added", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"payload": "abc", "code": 200, "add": "foo"})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, ErrUnexpectedField, nil, ErrUnexpectedField})
	})
	t.Run("removed", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{nil, nil, ErrMissingField, ErrMissingField})
	})
	t.Run("renamed", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"body": "abc", "code": 200})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF},
			[]error{nil, ErrUnexpectedField, ErrMissingField})
		checkExpectedErrorOptions(t, errFF, ErrUnexpectedField, ErrMissingField)
	})

	t.Run("changedType", func(t *testing.T) {
		errTT, errFT, errTF, errFF := testValidateRun(service, map[string]interface{}{"payload": "abc", "code": "ok"})
		checkExpectedErrors(
			t,
			[]error{errTT, errFT, errTF, errFF},
			[]error{ErrUnexpectedType, ErrUnexpectedType, ErrUnexpectedType, ErrUnexpectedType})
	})
}

func checkExpectedErrors(t *testing.T, actualErrors []error, expectedSentinels []error) {
	t.Helper()
	for i, err := range actualErrors {
		if !errors.Is(err, expectedSentinels[i]) {
			t.Fatal("got:", err, "expected:", expectedSentinels[i], "index:", i)
		}
	}
}

func checkExpectedError(t *testing.T, actualError error, expectedSentinel error) {
	t.Helper()
	if !errors.Is(actualError, expectedSentinel) {
		t.Fatal(actualError, expectedSentinel)
	}
}

func checkExpectedErrorOptions(t *testing.T, actualError error, expectedOptions ...error) {
	t.Helper()
	for _, option := range expectedOptions {
		if errors.Is(actualError, option) {
			return
		}
	}
	t.Fatal(actualError)
}

func testValidateRun(service model.Service, msg map[string]interface{}) (errTT error, errFT error, errTF error, errFF error) {
	errTT = Validate(msg, service, true, true)
	errFT = Validate(msg, service, false, true)
	errTF = Validate(msg, service, true, false)
	errFF = Validate(msg, service, false, false)
	return
}
