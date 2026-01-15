/*
 * Copyright 2024 InfAI (CC SES)
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

package iot

import (
	"log/slog"

	devicerepo "github.com/SENERGY-Platform/device-repository/lib/client"
	permv2 "github.com/SENERGY-Platform/permissions-v2/pkg/client"
)

type Iot struct {
	manager_url string
	repo_url    string
	devicerepo  devicerepo.Interface
	perm        permv2.Client
	logger      *slog.Logger
}

func New(deviceManagerUrl string, deviceRepoUrl string, permv2Url string, logger *slog.Logger) *Iot {
	return &Iot{
		manager_url: deviceManagerUrl,
		repo_url:    deviceRepoUrl,
		devicerepo:  devicerepo.NewClient(deviceRepoUrl, nil),
		perm:        permv2.New(permv2Url),
		logger:      logger,
	}
}

func (this *Iot) GetLogger() *slog.Logger {
	return this.logger
}
