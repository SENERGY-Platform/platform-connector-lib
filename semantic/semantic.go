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
	"github.com/SENERGY-Platform/platform-connector-lib/cache"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
)

type RepositoryInterface interface {
	GetCharacteristicById(id string, token security.JwtToken) (characteristic model.Characteristic, err error)
}

type Repository struct {
	cache                    *cache.Cache
	semanticRepositoryUrl    string
	characteristicExpiration int32
}

type RepositoryMock struct{}

func NewSemanticRepository(cacheUrls []string, semanticRepositoryUrl string, characteristicExpiration int32) RepositoryInterface {
	return &Repository{
		cache:                    cache.New(cacheUrls...),
		semanticRepositoryUrl:    semanticRepositoryUrl,
		characteristicExpiration: characteristicExpiration,
	}
}

func NewSemanticRepositoryMock() RepositoryInterface {
	return &RepositoryMock{}
}
