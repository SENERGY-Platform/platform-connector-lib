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
	"github.com/SENERGY-Platform/platform-connector-lib/semantic"
)

func FillUnitsInContent(content *model.Content, token security.JwtToken, semantic *semantic.SemanticRepository) (err error) {
	return fillUnitsForContentVariables(nil, &content.ContentVariable, content, token, semantic)
}

func fillUnitsForContentVariables(parent *model.ContentVariable, variable *model.ContentVariable, content *model.Content, token security.JwtToken, semantic *semantic.SemanticRepository) (err error) {
	if variable.SubContentVariables != nil || len(variable.SubContentVariables) > 0 {
		for _, subVariable := range variable.SubContentVariables {
			err = fillUnitsForContentVariables(variable, &subVariable, content, token, semantic)
			if err != nil {
				return err
			}
		}
	}

	if variable.UnitReference != "" {
		/*
			parent, err := findParentOfContentVariable(variable, content)
			if err != nil {
				return err
			}
		*/
		characteristicId, err := findCharacteristicIdOfChildWithName(parent, variable.UnitReference)
		if err != nil {
			return err
		}
		characteristic, err := semantic.GetCharacteristicById(characteristicId, token)
		if err != nil {
			return err
		}
		variable.Value = characteristic.Name
	}
	return nil
}

/*
func findParentOfContentVariable(search *model.ContentVariable, content *model.Content) (parent *model.ContentVariable, err error) {
	if content.ContentVariable.Id == search.Id {
		return nil, errors.New("ContentVariable with id " + search.Id + " is ContentVariable of Content with id " + content.Id)
	}
	return findParentOfContentVariableInternal(search, &content.ContentVariable, nil)
}

func findParentOfContentVariableInternal(search *model.ContentVariable, current *model.ContentVariable, currentParent *model.ContentVariable) (parent *model.ContentVariable, err error) {
	if current.Id == search.Id {
		return currentParent, nil
	}
	if current.SubContentVariables == nil || len(current.SubContentVariables) == 0 {
		return nil, errors.New("Reached bottom while looking for parent of ContentVariable with id " + search.Id)
	}
	for _, child := range current.SubContentVariables {
		parent, err = findParentOfContentVariableInternal(search, &child, current)
		if err != nil {
			return parent, err
		}
	}
	return nil, errors.New("Could not find parent of ContentVariable with id " + search.Id)
}

*/

func findCharacteristicIdOfChildWithName(parent *model.ContentVariable, searchName string) (characteristicId string, err error) {
	for _, neighbor := range parent.SubContentVariables {
		if neighbor.Name == searchName {
			return neighbor.CharacteristicId, nil
		}
	}
	return "", errors.New("Could not find child with name " + searchName + " for parent with id " + parent.Id)
}
