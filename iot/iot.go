package iot

type Iot struct {
	manager_url string
	repo_url    string
}

func New(deviceManagerUrl string, deviceRepoUrl string) *Iot {
	return &Iot{manager_url: deviceManagerUrl, repo_url: deviceRepoUrl}
}
