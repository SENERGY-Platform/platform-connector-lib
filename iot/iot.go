package iot

import "github.com/SENERGY-Platform/platform-connector-lib/statistics"

type Iot struct {
	manager_url           string
	repo_url              string
	semanticRepositoryUrl string
	permQueryUrl          string
	statistics            statistics.Interface
}

func New(deviceManagerUrl string, deviceRepoUrl string, semanticRepoUrl string, permQueryUrl string) *Iot {
	return &Iot{manager_url: deviceManagerUrl, repo_url: deviceRepoUrl, semanticRepositoryUrl: semanticRepoUrl, permQueryUrl: permQueryUrl, statistics: statistics.Void{}}
}

func (this *Iot) SetStatisticsCollector(collector statistics.Interface) *Iot {
	this.statistics = collector
	return this
}
