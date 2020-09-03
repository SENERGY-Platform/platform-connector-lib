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
	"fmt"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"strconv"
)

func RemoveUnknownFields(msg map[string]interface{}, service model.Service) (_ map[string]interface{}, err error) {
	for key, value := range msg {
		if variable, ok := fieldInService(key, service); ok {
			msg[key], err = removeUnknownField(value, variable)
			if err != nil {
				return msg, err
			}
		} else {
			delete(msg, key)
		}
	}
	return msg, nil
}

func removeUnknownField(value interface{}, variable model.ContentVariable) (_ interface{}, err error) {
	switch v := value.(type) {
	case string:
		if variable.Type != model.String {
			return nil, fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, model.String, variable.Type)
		}
		return value, nil
	case int:
		if variable.Type != model.Integer && variable.Type != model.Float {
			return nil, fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, model.Integer, variable.Type)
		}
		return value, nil
	case int64:
		if variable.Type != model.Integer && variable.Type != model.Float {
			return nil, fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, model.Integer, variable.Type)
		}
		return value, nil
	case float64:
		if variable.Type != model.Integer && variable.Type != model.Float {
			return nil, fmt.Errorf("%v: %w (is: %v, expected: %v) %v", variable.Name, ErrUnexpectedType, model.Float, variable.Type, v)
		}
		return value, nil
	case bool:
		if variable.Type != model.Boolean {
			return nil, fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, model.Boolean, variable.Type)
		}
		return value, nil
	case map[string]interface{}:
		if variable.Type != model.Structure {
			return nil, fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, model.Structure, variable.Type)
		}
		if variable.SubContentVariables[0].Name == "*" {
			for key, subValue := range v {
				v[key], err = removeUnknownField(subValue, variable.SubContentVariables[0])
				if err != nil {
					return v, err
				}
			}
		} else {
			for key, subValue := range v {
				if subVariable, ok := fieldInVariable(key, variable); ok {
					v[key], err = removeUnknownField(subValue, subVariable)
					if err != nil {
						return v, err
					}
				} else {
					delete(v, key)
				}
			}
		}
		return v, nil
	case []interface{}:
		if variable.Type != model.List {
			return nil, fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, model.List, variable.Type)
		}
		if variable.SubContentVariables[0].Name == "*" {
			for key, subValue := range v {
				v[key], err = removeUnknownField(subValue, variable.SubContentVariables[0])
				if err != nil {
					return v, err
				}
			}
		} else {
			for i, subValue := range v {
				if subVariable, ok := fieldInVariable(strconv.Itoa(i), variable); ok {
					v[i], err = removeUnknownField(subValue, subVariable)
					if err != nil {
						return v, err
					}
				} else {
					return v[:i], nil //if list element is not found -> return only known elements
				}
			}
		}
		return v, nil
	default:
		return value, nil
	}
}

func fieldInVariable(fieldName string, variable model.ContentVariable) (model.ContentVariable, bool) {
	for _, sub := range variable.SubContentVariables {
		if sub.Name == fieldName {
			return sub, true
		}
	}
	return model.ContentVariable{}, false
}

func fieldInService(fieldName string, service model.Service) (model.ContentVariable, bool) {
	for _, output := range service.Outputs {
		if output.ContentVariable.Name == fieldName {
			return output.ContentVariable, true
		}
	}
	return model.ContentVariable{}, false
}
