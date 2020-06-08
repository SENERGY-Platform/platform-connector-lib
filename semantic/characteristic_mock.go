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

package semantic

import (
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
)

func (this *RepositoryMock) GetCharacteristicById(id string, token security.JwtToken) (characteristic model.Characteristic, err error) {
	switch id {
	case "urn:infai:ses:characteristic:5b4eea52-e8e5-4e80-9455-0382f81a1b43":
		characteristic.Name = "RGB"
	case "urn:infai:ses:characteristic:64928e9f-98ca-42bb-a1e5-adf2a760a2f9":
		characteristic.Name = "HSB"
	}
	return characteristic, err
}
