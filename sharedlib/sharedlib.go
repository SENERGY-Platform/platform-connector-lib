package main

// #include <stdlib.h>
import "C"
import (
	"encoding/json"
	"github.com/SENERGY-Platform/formatter-lib"
	"github.com/SENERGY-Platform/platform-connector-lib"
	"github.com/SENERGY-Platform/platform-connector-lib/iot"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"strings"
)

func main() {}

var iotCache *iot.PreparedCache
var securityHandler *security.Security

//export Init
func Init(authEndpoint *C.char, authClientId *C.char, authClientSecret *C.char, iotRepoUrl *C.char, protocol *C.char, deviceExpiration int, deviceTypeExpiration int, iotCacheUrls *C.char) {

	iotCache = iot.NewCache(
		iot.New(C.GoString(iotRepoUrl), C.GoString(protocol)),
		int32(deviceExpiration),
		int32(deviceTypeExpiration),
		stringToList(C.GoString(iotCacheUrls))...,
	)

	securityHandler = security.New(
		C.GoString(authEndpoint),
		C.GoString(authClientId),
		C.GoString(authClientSecret),
		"",
		"",
		0,
		1,
		0,
		[]string{},
	)
}

//export Transform
func Transform(payload *C.char, deviceUri *C.char, serviceUri *C.char) (result *C.char) {
	if iotCache == nil || securityHandler == nil {
		return jsonHelper(map[string]string{"err": "call Init() before Transform()"})
	}

	var topic string
	var kafkaPayload string

	event := platform_connector_lib.EventMsg{}
	err := json.Unmarshal([]byte(C.GoString(payload)), &event)
	if err != nil {
		return jsonHelper(map[string]string{"err": err.Error()})
	}
	protocol := []model.ProtocolPart{}
	for segmentName, value := range event {
		protocol = append(protocol, model.ProtocolPart{Name: segmentName, Value: value})
	}

	token, err := securityHandler.Access()
	if err != nil {
		return jsonHelper(map[string]string{"err": err.Error()})
	}

	entities, err := iotCache.WithToken(token).DeviceUrlToIotDevice(C.GoString(deviceUri))
	if err != nil {
		return jsonHelper(map[string]string{"err": err.Error()})
	}

	for _, entity := range entities {
		for _, service := range entity.Services {
			if service.Url == C.GoString(serviceUri) {
				topic = strings.Replace(service.Id, "#", "_", -1)
				eventMsg := formatter_lib.EventMsg{}
				for _, part := range protocol {
					eventMsg = append(eventMsg, formatter_lib.ProtocolPart{Name: part.Name, Value: part.Value})
				}
				formatter, err := formatter_lib.NewTransformer(iotCache.WithToken(token), entity.Device.Id, service.Id)
				if err != nil {
					return jsonHelper(map[string]string{"err": err.Error()})
				}
				kafkaPayload, err = formatter.Transform(eventMsg)
				return jsonHelper(map[string]string{"topic": topic, "payload": kafkaPayload})
			}
		}
	}
	return jsonHelper(map[string]string{"topic": topic, "payload": kafkaPayload})
}

func jsonHelper(value interface{}) *C.char {
	temp, _ := json.Marshal(value)
	return C.CString(string(temp))
}

func stringToList(str string) []string {
	temp := strings.Split(str, ",")
	result := []string{}
	for _, e := range temp {
		trimmed := strings.TrimSpace(e)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
