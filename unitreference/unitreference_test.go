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
	"context"
	"github.com/SENERGY-Platform/platform-connector-lib/iot"
	iot2 "github.com/SENERGY-Platform/platform-connector-lib/iot/mock/iot"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"testing"
	"time"
)

func TestFillUnits(t *testing.T) {

	service := model.Service{
		Outputs: []model.Content{
			{
				ContentVariable: model.ContentVariable{
					Name: "complex",
					Type: model.Structure,
					SubContentVariables: []model.ContentVariable{
						{Name: "rgb", Type: model.String, Value: "str", CharacteristicId: "urn:infai:ses:characteristic:5b4eea52-e8e5-4e80-9455-0382f81a1b43"},
						{Name: "rgb_unit", Type: model.String, UnitReference: "rgb"},
						{Name: "map", Type: model.Structure, SubContentVariables: []model.ContentVariable{
							{Name: "hsb", Type: model.String, Value: "str2", CharacteristicId: "urn:infai:ses:characteristic:64928e9f-98ca-42bb-a1e5-adf2a760a2f9"},
							{Name: "hsb_unit", Type: model.String, UnitReference: "hsb"},
						}},
						{Name: "list", Type: model.List, SubContentVariables: []model.ContentVariable{
							{Name: "hsb", Type: model.String, Value: "str2", CharacteristicId: "urn:infai:ses:characteristic:64928e9f-98ca-42bb-a1e5-adf2a760a2f9"},
							{Name: "hsb_unit", Type: model.String, UnitReference: "hsb"},
						}},
					},
				}},
			{
				ContentVariable: model.ContentVariable{
					Name: "c2",
					Type: model.Structure,
					SubContentVariables: []model.ContentVariable{
						{Name: "rgb", Type: model.String, Value: "str", CharacteristicId: "urn:infai:ses:characteristic:5b4eea52-e8e5-4e80-9455-0382f81a1b43"},
						{Name: "rgb_unit", Type: model.String, UnitReference: "rgb"},
					},
				}},
		},
	}
	t.Run("fill", func(t *testing.T) {
		mock, repo, cancel, err := NewSemanticRepositoryMock()
		if err != nil {
			t.Error(err)
			return
		}
		defer cancel()
		err = SetMockCHaracteristics(mock)
		if err != nil {
			t.Error(err)
			return
		}

		err = FillUnitsForService(&service, "", repo)
		if err != nil {
			t.Error(err)
			return
		}

		if service.Outputs[0].ContentVariable.SubContentVariables[1].Value != "RGB" {
			t.Error("did not fill rgb")
		}
		if service.Outputs[1].ContentVariable.SubContentVariables[1].Value != "RGB" {
			t.Error("did not fill rgb")
		}
		if service.Outputs[0].ContentVariable.SubContentVariables[2].SubContentVariables[1].Value != "HSB" {
			t.Error("did not fill hsb")
		}
		if service.Outputs[0].ContentVariable.SubContentVariables[3].SubContentVariables[1].Value != "HSB" {
			t.Error("did not fill hsb")
		}
	})

}

func NewSemanticRepositoryMock() (mockCtrl *iot2.Controller, repo SemanticRepository, cancel func(), err error) {
	ctx, cancel := context.WithCancel(context.Background())
	mock, iotMockUrl, err := iot2.Mock(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	iotrepo := iot.New(iotMockUrl, iotMockUrl, iotMockUrl, "")
	cache := iot.NewCache(iotrepo, 60, 60, 60, 2, 200*time.Millisecond)
	return mock, cache, cancel, nil
}

func SetMockCHaracteristics(mockCtrl *iot2.Controller) (err error) {
	_, err, _ = mockCtrl.PublishCharacteristicUpdate("urn:infai:ses:characteristic:5b4eea52-e8e5-4e80-9455-0382f81a1b43", model.Characteristic{Id: "urn:infai:ses:characteristic:5b4eea52-e8e5-4e80-9455-0382f81a1b43", Name: "RGB"})
	if err != nil {
		return err
	}
	_, err, _ = mockCtrl.PublishCharacteristicUpdate("urn:infai:ses:characteristic:64928e9f-98ca-42bb-a1e5-adf2a760a2f9", model.Characteristic{Id: "urn:infai:ses:characteristic:64928e9f-98ca-42bb-a1e5-adf2a760a2f9", Name: "HSB"})
	if err != nil {
		return err
	}
	return nil
}
