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

package unitreference

import (
	"errors"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
)

func FillUnitsForService(service *model.Service, token security.JwtToken, semantic SemanticRepository) (err error) {
	for _, output := range service.Outputs {
		err = fillUnitsInContent(&output, token, semantic)
		if err != nil {
			return err
		}
	}
	return nil
}

func fillUnitsInContent(content *model.Content, token security.JwtToken, semantic SemanticRepository) (err error) {
	return fillUnitsForContentVariables(nil, -1, &content.ContentVariable, content, token, semantic)
}

func fillUnitsForContentVariables(parent *model.ContentVariable, parentIndex int, variable *model.ContentVariable,
	content *model.Content, token security.JwtToken, semantic SemanticRepository) (err error) {

	if variable == nil {
		return errors.New("variable may not be nil")
	}
	if variable.SubContentVariables != nil && len(variable.SubContentVariables) > 0 {
		for i, subVariable := range variable.SubContentVariables {
			err = fillUnitsForContentVariables(variable, i, &subVariable, content, token, semantic)
			if err != nil {
				return err
			}
		}
	}

	if variable.UnitReference != "" {
		if parent == nil {
			return errors.New("top level variables may not have unit references yet")
		}
		if parentIndex < 0 {
			return errors.New("invalid parent index")
		}
		characteristicId, err := findCharacteristicIdOfChildWithName(parent, variable.UnitReference)
		if err != nil {
			return err
		}
		characteristic, err := semantic.GetCharacteristicById(characteristicId, token)
		if err != nil {
			return err
		}
		parent.SubContentVariables[parentIndex].Value = characteristic.Name
	}
	return nil
}

func findCharacteristicIdOfChildWithName(parent *model.ContentVariable, searchName string) (characteristicId string, err error) {
	for _, neighbor := range parent.SubContentVariables {
		if neighbor.Name == searchName {
			return neighbor.CharacteristicId, nil
		}
	}
	return "", errors.New("Could not find child with name " + searchName + " for parent with id " + parent.Id)
}
