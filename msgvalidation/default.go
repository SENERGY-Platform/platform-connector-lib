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

func DefaultMissingFields(msg map[string]interface{}, service model.Service) (result map[string]interface{}, err error) {
	for _, output := range service.Outputs {
		if value, ok := msg[output.ContentVariable.Name]; ok {
			msg[output.ContentVariable.Name], err = defaultMissingField(value, output.ContentVariable)
		} else {
			msg[output.ContentVariable.Name] = output.ContentVariable.Value
		}
	}
	return msg, nil
}

func defaultMissingField(value interface{}, variable model.ContentVariable) (_ interface{}, err error) {
	switch v := value.(type) {
	case string:
		if variable.Type != model.String {
			return nil, fmt.Errorf("%v: %w (is: %v, expected: %v) %v", variable.Name, ErrUnexpectedType, model.String, variable.Type, v)
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
				v[key], err = defaultMissingField(subValue, variable.SubContentVariables[0])
				if err != nil {
					return v, err
				}
			}
		} else {
			for _, subVariable := range variable.SubContentVariables {
				if subValue, ok := v[subVariable.Name]; ok {
					v[subVariable.Name], err = defaultMissingField(subValue, subVariable)
				} else {
					v[subVariable.Name] = subVariable.Value
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
				v[key], err = defaultMissingField(subValue, variable.SubContentVariables[0])
				if err != nil {
					return v, err
				}
			}
		} else {
			for _, subVariable := range variable.SubContentVariables {
				index, err := strconv.Atoi(subVariable.Name)
				if err != nil {
					return nil, fmt.Errorf("list variable name expected to be * or a number. got %v in %v", subVariable.Name, variable.Name)
				}
				if index < len(v) {
					v[index], err = defaultMissingField(v[index], subVariable)
				} else {
					v = append(v, subVariable.Value)
				}
			}
		}
		return v, nil
	default:
		return value, nil
	}
}
