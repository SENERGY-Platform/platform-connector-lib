package iot

type Iot struct {
	manager_url           string
	repo_url              string
	semanticRepositoryUrl string
	permQueryUrl          string
}

func New(deviceManagerUrl string, deviceRepoUrl string, semanticRepoUrl string, permQueryUrl string) *Iot {
	return &Iot{manager_url: deviceManagerUrl, repo_url: deviceRepoUrl, semanticRepositoryUrl: semanticRepoUrl, permQueryUrl: permQueryUrl}
}
