/*
 * Copyright 2018 InfAI (CC SES)
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

package platform_connector_lib

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	KafkaUrl           string
	KafkaResponseTopic string
	KafkaGroupName     string
	FatalKafkaError    bool
	Protocol           string

	DeviceManagerUrl string
	DeviceRepoUrl    string

	AuthClientId             string //keycloak-client
	AuthClientSecret         string //keycloak-secret
	AuthExpirationTimeBuffer float64
	AuthEndpoint             string

	JwtPrivateKey string
	JwtExpiration int64
	JwtIssuer     string

	DeviceExpiration      int32
	DeviceTypeExpiration  int32
	TokenCacheExpiration  int32
	IotCacheUrl           []string
	TokenCacheUrl         []string
	Debug                 bool
	SerializationFallback string

	Validate                  bool
	ValidateAllowUnknownField bool
	ValidateAllowMissingField bool

	CharacteristicExpiration int32
	PartitionsNum            int
	ReplicationFactor        int

	PublishToPostgres bool
	PostgresHost      string
	PostgresPort      int
	PostgresUser      string
	PostgresPw        string
	PostgresDb        string

	HttpCommandConsumerPort string

	PermQueryUrl string

	AsyncPgThreadMax    int
	AsyncFlushMessages  int
	AsyncFlushFrequency time.Duration
	AsyncCompression    sarama.CompressionCodec
	SyncCompression     sarama.CompressionCodec

	KafkaConsumerMaxWait  string
	KafkaConsumerMinBytes int
	KafkaConsumerMaxBytes int

	IotCacheTimeout      string
	IotCacheMaxIdleConns int

	NotificationUrl string

	DeviceTypeTopic string

	KafkaTopicConfigs map[string][]kafka.ConfigEntry

	NotificationsIgnoreDuplicatesWithinS int
	NotificationUserOverwrite            string
}

// loads config from json in location and used environment variables (e.g KafkaUrl --> ZOOKEEPER_URL)
func LoadConfig(location string) (config Config, err error) {
	file, error := os.Open(location)
	if error != nil {
		log.Println("error on config load: ", error)
		return config, error
	}
	decoder := json.NewDecoder(file)
	error = decoder.Decode(&config)
	if error != nil {
		log.Println("invalid config json: ", error)
		return config, error
	}
	handleEnvironmentVars(&config)
	return config, nil
}

var camel = regexp.MustCompile("(^[^A-Z]*|[A-Z]*)([A-Z][^A-Z]+|$)")

func fieldNameToEnvName(s string) string {
	var a []string
	for _, sub := range camel.FindAllStringSubmatch(s, -1) {
		if sub[1] != "" {
			a = append(a, sub[1])
		}
		if sub[2] != "" {
			a = append(a, sub[2])
		}
	}
	return strings.ToUpper(strings.Join(a, "_"))
}

// preparations for docker
func handleEnvironmentVars(config *Config) {
	configValue := reflect.Indirect(reflect.ValueOf(config))
	configType := configValue.Type()
	for index := 0; index < configType.NumField(); index++ {
		fieldName := configType.Field(index).Name
		envName := fieldNameToEnvName(fieldName)
		envValue := os.Getenv(envName)
		if envValue != "" {
			fmt.Println("use environment variable: ", envName, " = ", envValue)
			if configValue.FieldByName(fieldName).Kind() == reflect.Int64 {
				i, _ := strconv.ParseInt(envValue, 10, 64)
				configValue.FieldByName(fieldName).SetInt(i)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.String {
				configValue.FieldByName(fieldName).SetString(envValue)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Bool {
				b, _ := strconv.ParseBool(envValue)
				configValue.FieldByName(fieldName).SetBool(b)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Float64 {
				f, _ := strconv.ParseFloat(envValue, 64)
				configValue.FieldByName(fieldName).SetFloat(f)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Slice {
				val := []string{}
				for _, element := range strings.Split(envValue, ",") {
					val = append(val, strings.TrimSpace(element))
				}
				configValue.FieldByName(fieldName).Set(reflect.ValueOf(val))
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Map {
				value := map[string]string{}
				for _, element := range strings.Split(envValue, ",") {
					keyVal := strings.Split(element, ":")
					key := strings.TrimSpace(keyVal[0])
					val := strings.TrimSpace(keyVal[1])
					value[key] = val
				}
				configValue.FieldByName(fieldName).Set(reflect.ValueOf(value))
			}
		}
	}
}
