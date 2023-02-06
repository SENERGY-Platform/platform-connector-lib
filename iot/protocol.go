package iot

import (
	"encoding/json"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"github.com/SENERGY-Platform/platform-connector-lib/statistics"
	"log"
	"net/url"
	"time"
)

func (this *Iot) GetProtocol(id string, token security.JwtToken) (protocol model.Protocol, err error) {
	start := time.Now()
	defer statistics.IotRead(time.Since(start))
	resp, err := token.Get(this.repo_url + "/protocols/" + url.QueryEscape(id))
	if err != nil {
		log.Println("ERROR on GetProtocol()", err)
		return protocol, err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&protocol)
	if err != nil {
		log.Println("ERROR on GetProtocol() json decode", err)
	}
	return protocol, err
}
