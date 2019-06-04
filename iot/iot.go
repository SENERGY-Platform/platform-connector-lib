package iot

type Iot struct {
	semantic_url string
	repo_url     string
	protocol     string
}

func New(iotRepoUrl string, deviceRepoUrl string, protocolHandlerUri string) *Iot {
	return &Iot{semantic_url: iotRepoUrl, repo_url: deviceRepoUrl, protocol: protocolHandlerUri}
}
