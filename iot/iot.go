package iot

type Iot struct {
	url      string
	protocol string
}

func New(iotRepoUrl string, protocolHandlerUri string) *Iot {
	return &Iot{url: iotRepoUrl, protocol: protocolHandlerUri}
}
