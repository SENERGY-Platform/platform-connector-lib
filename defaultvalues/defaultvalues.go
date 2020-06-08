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
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
)

var ErrUnexpectedType = errors.New("unexpected type")

func FillDefaultValues(msg *map[string]interface{}, service model.Service) (err error) {
	for _, content := range service.Outputs {
		variable := content.ContentVariable
		segment, ok := (*msg)[variable.Name]
		if ok {
			err := fillDefaultValuesVariable(&segment, variable)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func fillDefaultValuesVariable(segment *interface{}, variable model.ContentVariable) (err error) {
	if segment == nil || *segment == nil { //in a nested value we don't fill sub values if parent is already nil
		if segment == nil {
			var tmp interface{}
			segment = &tmp
		}

		if variable.Value != nil {
			*segment = variable.Value
		}
		return nil
	}

	if variable.SubContentVariables != nil || len(variable.SubContentVariables) > 0 {
		switch variable.Type {
		case model.Structure:
			v, ok := (*segment).(map[string]interface{})
			if !ok {
				return fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, model.Structure, variable.Type)
			}

			if variable.SubContentVariables[0].Name == "*" {
				for _, subSegment := range v {
					err := fillDefaultValuesVariable(&subSegment, variable.SubContentVariables[0])
					if err != nil {
						return err
					}
				}
			} else {
				for _, variable := range variable.SubContentVariables {
					subSegment, ok := v[variable.Name]

					err := fillDefaultValuesVariable(&subSegment, variable)
					if err != nil {
						return err
					}
					if !ok && subSegment != nil {
						v[variable.Name] = subSegment
					}

				}
			}
		case model.List:

			v, ok := (*segment).([]interface{})
			if !ok {
				return fmt.Errorf("%v: %w (is: %v, expected: %v)", variable.Name, ErrUnexpectedType, model.Structure, variable.Type)
			}

			if variable.SubContentVariables[0].Name == "*" {
				for _, subSegment := range v {
					err := fillDefaultValuesVariable(&subSegment, variable.SubContentVariables[0])
					if err != nil {
						return err
					}
				}
			} else {
				for i, subVar := range variable.SubContentVariables {
					if i < len(v) {
						subSegment := v[i]
						err := fillDefaultValuesVariable(&subSegment, subVar)
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return nil
}
