package iot

import "github.com/SENERGY-Platform/permission-search/lib/client"

type Iot struct {
	manager_url string
	repo_url    string
	permissions client.Client
}

func New(deviceManagerUrl string, deviceRepoUrl string, permQueryUrl string) *Iot {
	return &Iot{manager_url: deviceManagerUrl, repo_url: deviceRepoUrl, permissions: client.NewClient(permQueryUrl)}
}
