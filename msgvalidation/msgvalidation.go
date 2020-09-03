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
	"fmt"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
)

var ErrUnexpectedField = errors.New("unexpected field")
var ErrMissingField = errors.New("missing field")
var ErrUnexpectedType = errors.New("unexpected type")

func Validate(msg map[string]interface{}, service model.Service, allowAdditionalFields bool, allowMissingFields bool) error {
	outputNames := map[string]bool{}
	for _, content := range service.Outputs {
		variable := content.ContentVariable
		outputNames[variable.Name] = true
		segment, ok := msg[variable.Name]
		if ok {
			err := ValidateMsgVariable(segment, variable, allowAdditionalFields, allowMissingFields)
			if err != nil {
				return err
			}
		} else if !allowMissingFields {
			return fmt.Errorf("%v: %w", variable.Name, ErrMissingField)
		}
	}
	if !allowAdditionalFields {
		for field, _ := range msg {
			if !outputNames[field] {
				return fmt.Errorf("%v: %w", field, ErrUnexpectedField)
			}
		}
	}
	return nil
}

func ValidateMsgVariable(segment interface{}, variable model.ContentVariable, allowAdditionalFields bool, allowMissingFields bool) error {
	if segment == nil {
		if allowMissingFields {
			return nil
		} else {
			return fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, "null", variable.Type)
		}
	}
	switch v := segment.(type) {
	case string:
		if variable.Type != model.String {
			return fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, model.String, variable.Type)
		}
	case int:
		if variable.Type != model.Integer && variable.Type != model.Float {
			return fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, model.Integer, variable.Type)
		}
	case int64:
		if variable.Type != model.Integer && variable.Type != model.Float {
			return fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, model.Integer, variable.Type)
		}
	case float64:
		if variable.Type != model.Integer && variable.Type != model.Float {
			return fmt.Errorf("%v: %w (is: %v, expected: %v) %v", variable.Name, ErrUnexpectedType, model.Float, variable.Type, v)
		}
	case bool:
		if variable.Type != model.Boolean {
			return fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, model.Boolean, variable.Type)
		}
	case map[string]interface{}:
		if variable.Type != model.Structure {
			return fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, model.Structure, variable.Type)
		}
		if variable.SubContentVariables[0].Name == "*" {
			for _, segment := range v {
				err := ValidateMsgVariable(segment, variable.SubContentVariables[0], allowAdditionalFields, allowMissingFields)
				if err != nil {
					return err
				}
			}
		} else {
			subNames := map[string]bool{}
			for _, variable := range variable.SubContentVariables {
				subNames[variable.Name] = true
				segment, ok := v[variable.Name]
				if ok {
					err := ValidateMsgVariable(segment, variable, allowAdditionalFields, allowMissingFields)
					if err != nil {
						return err
					}
				} else if !allowMissingFields {
					return fmt.Errorf("%v: %w", variable.Name, ErrMissingField)
				}
			}
			if !allowAdditionalFields {
				for field, _ := range v {
					if !subNames[field] {
						return fmt.Errorf("%v: %w", field, ErrUnexpectedField)
					}
				}
			}
		}
	case []interface{}:
		if variable.Type != model.List {
			return fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, model.List, variable.Type)
		}
		if variable.SubContentVariables[0].Name == "*" {
			for _, segment := range v {
				err := ValidateMsgVariable(segment, variable.SubContentVariables[0], allowAdditionalFields, allowMissingFields)
				if err != nil {
					return err
				}
			}
		} else {
			if len(v) > len(variable.SubContentVariables) && !allowAdditionalFields {
				return fmt.Errorf("%v: %w (list size)", variable.Name, ErrUnexpectedField)
			}
			if len(v) < len(variable.SubContentVariables) && !allowMissingFields {
				return fmt.Errorf("%v: %w (list size)", variable.Name, ErrMissingField)
			}
			for i, subVar := range variable.SubContentVariables {
				if i < len(v) {
					segment := v[i]
					err := ValidateMsgVariable(segment, subVar, allowAdditionalFields, allowMissingFields)
					if err != nil {
						return err
					}
				}
			}
		}
	default:
		return fmt.Errorf("%v: %w", variable.Name, ErrUnexpectedType)
	}
	return nil
}
