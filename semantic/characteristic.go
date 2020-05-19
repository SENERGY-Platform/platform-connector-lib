package semantic

import (
	"encoding/json"
	platform_connector_lib "github.com/SENERGY-Platform/platform-connector-lib"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"io/ioutil"
)

func GetCharacteristicById(id string, config platform_connector_lib.Config, token security.JwtToken) (characteristic model.Characteristic, err error) {
	resp, err := token.Get(config.SemanticRepositoryUrl + "/characteristics/" + id)
	if err != nil {
		return characteristic, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return characteristic, err
	}
	err = json.Unmarshal(body, &characteristic)
	return characteristic, err
}
