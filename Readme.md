Library to create a platform-connector to handle a protocol which communicates with devices


## Init
```Init(config Config, commandHandler CommandHandler)(connector *Connector)```

Creates an instance of Connector. Expects a config and a command handler. 

```
configLocation := flag.String("config", "config.json", "configuration file")
flag.Parse()

libConf, err := platform_connector_lib.LoadConfig(*configLocation)
if err != nil {
    log.Fatal(err)
}

connector := platform_connector_lib.Init(libConf, func(endpoint string, protocolParts map[string]string) (responseParts map[string]string, err error) {
    responseParts = map[string]string{}
    err = MqttPublish(endpoint, protocolParts["payload"])
    return
})

defer connector.Stop()
```

## Stop
```(this *Connector) Stop()```

Disconnects connector gracefully. 

## Commnand-Handler
```type CommandHandler func(endpoint string, protocolParts map[string]string)(responseParts map[string]string, err error)```

The command-handler is a function that is passed to Init(). It receives commands from the sepl-platform and should transmit them to the application/protocol.

### input/output
* endpoint: identifies device and service for the protocol
* protocolParts: map from protocol part to string representation of information that should be transmitted. for example `{"body":"{\"foo\":\"bar\"}", "header": "foobar"}`. the parts and there handling are protocol specific.
* responseParts: equivalent to protocolParts but as the response from the device. Should have usually the same keys as the protocolParts.


## Event-Handling
```(this *Connector) HandleEvent(username string, password string, endpoint string, protocolParts map[string]string)(total int, success int, ignore int, fail int, err error)```

If your application/protocol receives a event with credentials it can call HandleEvent to relay the message to the platform. 

```(this *Connector) HandleEventWithAuthToken(token JwtToken, endpoint string, protocolParts map[string]string)(total int, success int, ignore int, fail int, err error)```

if your application handles long-term connections and has saved a access-token for the user, you can call HandleEventWithAuthToken.

### input/output
* username
* password
* token: acces-token from GetUserToken() or OpenidToken::JwtToken()
* endpoint: identifies device and service in the protocol
* total: number of matching endpoints found in the sepl-platform
* success: number of successful used endpoints
* ignore: number of ignored endpoints (endpoint is not for a sensor)
* fail: number of endpoints that could not be used, because of an error
* err: error while creating or finding endpoints
* protocolParts: map from protocol part to string representation of information that should be transmitted. for example `{"body":"{\"foo\":\"bar\"}", "header": "foobar"}`. the parts and there handling are protocol specific.

### endpoint
A endpoint references a device-specific instantiation of iot-device-repository.Service.EndpointFormat. The endpoint identifies device-service combinations. 
When no matching endpoint can be found, a new device will be created. If no devicetype with a matching service signature and valuetype can be found, new ones will be created.
The iot-device-repository.Service.EndpointFormat is a mustache template which may reference device and service uri.
For example `/some/path/{{device_uri}}/foo/{{service_uri}}/bar`.
The use of the device uri is highly advised, to ensure that for a devicetype each device has its own service endpoints.
The last example used a structure reminiscent of a http-path, but it can be everything. It is advisable to use one that prevents ambiguous endpoints but it may in fact be somthing like `iuhansdcansdc{{device_uri}}asidauisdha`.

Endpoints will only be evaluated for devices the user as execution rights to and the IoT-Repository prevents endpoint-collisions. 

If iot-device-repository.Service.EndpointFormat is empty the iot-repository will create no endpoint for this device-service combination.

## Auth
If you want to save and handle user-auth-tokens you can use the following functions:
* `(this *Connector) CheckEndpointAuth(token JwtToken, endpoint string) (err error)`: 
asks the iot-repo if the user is allowed to access the endpoint with execution rights.
* `(this *Connector) GetUserToken(username string) (token JwtToken, err error)`: 
creates a jwt for this user without calling keycloak.
* `(this *Connector) GetOpenidPasswordToken(username, password string) (token OpenidToken, err error)`:
gets access and refresh token information from keycloak.
* `(this *OpenidToken) JwtToken() JwtToken`:
gets a usable jwt from the GetOpenidPasswordToken() response.

the auth token can be used to do http-requests in the name of the user:
* `(this JwtToken) Post(url string, contentType string, body io.Reader) (resp *http.Response, err error)`
* `(this JwtToken) PostJSON(url string, body interface{}, result interface{}) (err error)`
* `(this JwtToken) Get(url string) (resp *http.Response, err error)` 
* `(this JwtToken) GetJSON(url string, result interface{}) (err error)`


## Config
Use `LoadConfig(location string) (config Config, err error)` to read a configuration or instantiate is manually to pass it tio the Init() function.
 
* ZookeeperUrl: url to the zookeeper service
* KafkaResponseTopic: kafka topic to which command-responses should be send (`"response"` for the sepl-platform)
* KafkaEventTopic: kafka topic to which event-messages should be send (`"eventfilter"` for the sepl-platform). Events will also be send to the topic `strings.Replace(serviceId, "#", "_", -1)`.
* Protocol: references iot-device-repository.Protocol.ProtocolHandlerUrl. Is used as topic to consume device-commands on this protocol.
* FatalKafkaErrors: if `"true"` the application will terminate if an kafka-error occurs.
* KafkaTimeout: if > 0 the connector will send messages with `"topic_init"` to the protocol topic to ensure connection. if in the timeout span no message is received a fatal error will occur.
the `"topic_init"` message will be ignored by all sepl-kafka-consumers.
* SaramaLog:  if `"true"` the Kafka-Library Sarama will write its logs to the standart out stream.
* IotRepoUrl: url to the iot-repo.
* LogProduce: if `"true"`, log kafka-produce calls.
* AuthClientId: keycloak-client-id of this connector.
* AuthClientSecret: keycloak-client-secret of this connector.
* AuthExpirationTimeBuffer: time buffer in seconds. if the access-token expires in less then this time, it will be refreshed.
* AuthEndpoint: url of keycloak.
* JwtPrivateKey: if set, GetUserToken() will use it to sign the resulting jwt.
* JwtExpiration: expiration time of the jwt created by GetUserToken().
* JwtIssuer: issuer of the jwt created by GetUserToken().

 
```
type Config struct {
	ZookeeperUrl       string //host1:2181,host2:2181/chroot
	KafkaResponseTopic string
	KafkaEventTopic    string
	Protocol           string
	FatalKafkaErrors   string
	KafkaTimeout       int64
	SaramaLog          string

	IotRepoUrl   string

	LogProduce	 string //"true"

	AuthClientId             string //keycloak-client
	AuthClientSecret         string //keycloak-secret
	AuthExpirationTimeBuffer float64
	AuthEndpoint             string
	JwtPrivateKey            string
	JwtExpiration            int64
	JwtIssuer                string
}
```
