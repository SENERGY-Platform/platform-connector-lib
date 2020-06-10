/*
 * Copyright 2020 InfAI (CC SES)
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

package msgvalidation

import (
	"encoding/json"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"reflect"
	"testing"
)

func TestClean(t *testing.T) {
	t.Run("simple exact match", cleanTest(SimpleValueService, `{"state": "on"}`, `{"state": "on"}`))
	t.Run("simple missing", cleanTest(SimpleValueService, `{}`, `{"state": null}`))
	t.Run("simple to many", cleanTest(SimpleValueService, `{"state": "on", "foo":"bar"}`, `{"state": "on"}`))
	t.Run("simple to wrong name", cleanTest(SimpleValueService, `{"status": "on"}`, `{"state": null}`))

	t.Run("complex exact match", cleanTest(StructValueService, `{"struct": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}`, `{"struct": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}`))
	t.Run("complex null", cleanTest(StructValueService, `{"struct": {"hue": null, "on": null, "time": null}}`, `{"struct": {"hue": null, "on": null, "time": null}}`))
	t.Run("complex root null", cleanTest(StructValueService, `{"struct": null}`, `{"struct": null}`))
	t.Run("complex missing int", cleanTest(StructValueService, `{"struct": {"on": true, "time": "13:00:00 UTC"}}`, `{"struct": {"hue": null, "on": true, "time": "13:00:00 UTC"}}`))
	t.Run("complex missing bool", cleanTest(StructValueService, `{"struct": {"hue": 42, "time": "13:00:00 UTC"}}`, `{"struct": {"hue": 42, "on": null, "time": "13:00:00 UTC"}}`))
	t.Run("complex missing text", cleanTest(StructValueService, `{"struct": {"hue": 42, "on": true}}`, `{"struct": {"hue": 42, "on": true, "time": null}}`))
	t.Run("complex unexpected float", cleanTest(StructValueService, `{"struct": {"hue": 42, "on": true, "time": "13:00:00 UTC", "foo":42.13}}`, `{"struct": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}`))
	t.Run("complex wrong name", cleanTest(StructValueService, `{"struct": {"foo": 42, "on": true, "time": "13:00:00 UTC"}}`, `{"struct": {"hue": null, "on": true, "time": "13:00:00 UTC"}}`))

	t.Run("nested exact match", cleanTest(NestedStructValueService, `{"struct": {"struct": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}}`, `{"struct": {"struct": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}}`))
	t.Run("nested missing int", cleanTest(NestedStructValueService, `{"struct": {"struct": {"on": true, "time": "13:00:00 UTC"}}}`, `{"struct": {"struct": {"hue": null, "on": true, "time": "13:00:00 UTC"}}}`))
	t.Run("nested missing bool", cleanTest(NestedStructValueService, `{"struct": {"struct": {"hue": 42, "time": "13:00:00 UTC"}}}`, `{"struct": {"struct": {"hue": 42, "on": null, "time": "13:00:00 UTC"}}}`))
	t.Run("nested missing text", cleanTest(NestedStructValueService, `{"struct": {"struct": {"hue": 42, "on": true}}}`, `{"struct": {"struct": {"hue": 42, "on": true, "time": null}}}`))
	t.Run("nested unexpected float", cleanTest(NestedStructValueService, `{"struct": {"struct": {"hue": 42, "on": true, "time": "13:00:00 UTC", "foo":42.13}}}`, `{"struct": {"struct": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}}`))
	t.Run("nested wrong name", cleanTest(NestedStructValueService, `{"struct": {"struct": {"foo": 42, "on": true, "time": "13:00:00 UTC"}}}`, `{"struct": {"struct": {"hue": null, "on": true, "time": "13:00:00 UTC"}}}`))

	t.Run("map exact match", cleanTest(MapStructValueService, `{"struct": {"foo": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}}`, `{"struct": {"foo": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}}`))
	t.Run("map missing element", cleanTest(MapStructValueService, `{"struct": null}`, `{"struct": null}`))
	t.Run("map missing int", cleanTest(MapStructValueService, `{"struct": {"foo": {"on": true, "time": "13:00:00 UTC"}}}`, `{"struct": {"foo": {"hue": null, "on": true, "time": "13:00:00 UTC"}}}`))
	t.Run("map missing bool", cleanTest(MapStructValueService, `{"struct": {"foo": {"hue": 42, "time": "13:00:00 UTC"}}}`, `{"struct": {"foo": {"hue": 42, "on": null, "time": "13:00:00 UTC"}}}`))
	t.Run("map missing text", cleanTest(MapStructValueService, `{"struct": {"foo": {"hue": 42, "on": true}}}`, `{"struct": {"foo": {"hue": 42, "on": true, "time": null}}}`))
	t.Run("map unexpected float", cleanTest(MapStructValueService, `{"struct": {"foo": {"hue": 42, "on": true, "time": "13:00:00 UTC", "foo":42.13}}}`, `{"struct": {"foo": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}}`))
	t.Run("map wrong name", cleanTest(MapStructValueService, `{"struct": {"foo": {"foo": 42, "on": true, "time": "13:00:00 UTC"}}}`, `{"struct": {"foo": {"hue": null, "on": true, "time": "13:00:00 UTC"}}}`))
	t.Run("map 2 elements", cleanTest(MapStructValueService, `{"struct": {"foo": {"hue": 42, "on": true, "time": "13:00:00 UTC"}, "bar": {"hue": 13}}}`, `{"struct": {"foo": {"hue": 42, "on": true, "time": "13:00:00 UTC"}, "bar": {"hue": 13, "on": null, "time": null}}}`))

	t.Run("list exact match", cleanTest(ListStructValueService, `{"list": [{"hue": 42, "on": true, "time": "13:00:00 UTC"}]}`, `{"list": [{"hue": 42, "on": true, "time": "13:00:00 UTC"}]}`))
	t.Run("list missing str", cleanTest(ListStructValueService, `{"list": [{"hue": 42, "on": true}]}`, `{"list": [{"hue": 42, "on": true, "time": null}]}`))
	t.Run("list missing int", cleanTest(ListStructValueService, `{"list": [{"on": true, "time": "13:00:00 UTC"}]}`, `{"list": [{"hue": null, "on": true, "time": "13:00:00 UTC"}]}`))
	t.Run("list missing bool", cleanTest(ListStructValueService, `{"list": [{"hue": 42, "time": "13:00:00 UTC"}]}`, `{"list": [{"hue": 42, "on": null, "time": "13:00:00 UTC"}]}`))
	t.Run("list extra field", cleanTest(ListStructValueService, `{"list": [{"foo":"bar", "hue": 42, "on": true, "time": "13:00:00 UTC"}]}`, `{"list": [{"hue": 42, "on": true, "time": "13:00:00 UTC"}]}`))
	t.Run("list 2 elements", cleanTest(ListStructValueService, `{"list": [{"hue": 42}, {"hue": 13}]}`, `{"list": [{"hue": 42, "on": null, "time": null},{"hue": 13, "on": null, "time": null}]}`))
	t.Run("list null", cleanTest(ListStructValueService, `{"list": null}`, `{"list": null}`))
	t.Run("list missing", cleanTest(ListStructValueService, `{}`, `{"list": null}`))

	t.Run("array exact match", cleanTest(ArrayValueService, `{"list": [42, true, "13:00:00 UTC"]}`, `{"list": [42, true, "13:00:00 UTC"]}`))
	t.Run("array 1 missing", cleanTest(ArrayValueService, `{"list": [42, true]}`, `{"list": [42, true, null]}`))
	t.Run("array 2 missing", cleanTest(ArrayValueService, `{"list": [42]}`, `{"list": [42, null, null]}`))
	t.Run("array 3 missing", cleanTest(ArrayValueService, `{"list": []}`, `{"list": [null, null, null]}`))
	t.Run("array null", cleanTest(ArrayValueService, `{"list": null}`, `{"list": null}`))
	t.Run("array missing", cleanTest(ArrayValueService, `{}`, `{"list": null}`))
	t.Run("array extra", cleanTest(ArrayValueService, `{"list": [42, true, "13:00:00 UTC", "foo"]}`, `{"list": [42, true, "13:00:00 UTC"]}`))
}

func TestCleanWithDefaults(t *testing.T) {
	t.Run("simple exact match", cleanTest(SimpleValueServiceWithDefault, `{"state": "on"}`, `{"state": "on"}`))
	t.Run("simple missing", cleanTest(SimpleValueServiceWithDefault, `{}`, `{"state": "ON"}`))
	t.Run("simple to many", cleanTest(SimpleValueServiceWithDefault, `{"state": "on", "foo":"bar"}`, `{"state": "on"}`))
	t.Run("simple to wrong name", cleanTest(SimpleValueServiceWithDefault, `{"status": "on"}`, `{"state": "ON"}`))

	t.Run("complex exact match", cleanTest(StructValueServiceWithDefault, `{"struct": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}`, `{"struct": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}`))
	t.Run("complex null", cleanTest(StructValueServiceWithDefault, `{"struct": {"hue": null, "on": null, "time": null}}`, `{"struct": {"hue": null, "on": null, "time": null}}`))
	t.Run("complex root null", cleanTest(StructValueServiceWithDefault, `{"struct": null}`, `{"struct": null}`))
	t.Run("complex root undefined", cleanTest(StructValueServiceWithDefault, `{}`, `{"struct": null}`))
	t.Run("complex missing int", cleanTest(StructValueServiceWithDefault, `{"struct": {"on": true, "time": "13:00:00 UTC"}}`, `{"struct": {"hue": 7, "on": true, "time": "13:00:00 UTC"}}`))
	t.Run("complex missing bool", cleanTest(StructValueServiceWithDefault, `{"struct": {"hue": 42, "time": "13:00:00 UTC"}}`, `{"struct": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}`))
	t.Run("complex missing text", cleanTest(StructValueServiceWithDefault, `{"struct": {"hue": 42, "on": true}}`, `{"struct": {"hue": 42, "on": true, "time": "default"}}`))
	t.Run("complex unexpected float", cleanTest(StructValueServiceWithDefault, `{"struct": {"hue": 42, "on": true, "time": "13:00:00 UTC", "foo":42.13}}`, `{"struct": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}`))
	t.Run("complex wrong name", cleanTest(StructValueServiceWithDefault, `{"struct": {"foo": 42, "on": true, "time": "13:00:00 UTC"}}`, `{"struct": {"hue": 7, "on": true, "time": "13:00:00 UTC"}}`))

	t.Run("nested exact match", cleanTest(NestedStructValueServiceWithDefault, `{"struct": {"struct": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}}`, `{"struct": {"struct": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}}`))
	t.Run("nested missing int", cleanTest(NestedStructValueServiceWithDefault, `{"struct": {"struct": {"on": true, "time": "13:00:00 UTC"}}}`, `{"struct": {"struct": {"hue": 7, "on": true, "time": "13:00:00 UTC"}}}`))
	t.Run("nested missing bool", cleanTest(NestedStructValueServiceWithDefault, `{"struct": {"struct": {"hue": 42, "time": "13:00:00 UTC"}}}`, `{"struct": {"struct": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}}`))
	t.Run("nested missing text", cleanTest(NestedStructValueServiceWithDefault, `{"struct": {"struct": {"hue": 42, "on": true}}}`, `{"struct": {"struct": {"hue": 42, "on": true, "time": "default"}}}`))
	t.Run("nested unexpected float", cleanTest(NestedStructValueServiceWithDefault, `{"struct": {"struct": {"hue": 42, "on": true, "time": "13:00:00 UTC", "foo":42.13}}}`, `{"struct": {"struct": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}}`))
	t.Run("nested wrong name", cleanTest(NestedStructValueServiceWithDefault, `{"struct": {"struct": {"foo": 42, "on": true, "time": "13:00:00 UTC"}}}`, `{"struct": {"struct": {"hue": 7, "on": true, "time": "13:00:00 UTC"}}}`))

	t.Run("map exact match", cleanTest(MapStructValueServiceWithDefault, `{"struct": {"foo": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}}`, `{"struct": {"foo": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}}`))
	t.Run("map missing element", cleanTest(MapStructValueServiceWithDefault, `{"struct": null}`, `{"struct": null}`))
	t.Run("map missing int", cleanTest(MapStructValueServiceWithDefault, `{"struct": {"foo": {"on": true, "time": "13:00:00 UTC"}}}`, `{"struct": {"foo": {"hue": 7, "on": true, "time": "13:00:00 UTC"}}}`))
	t.Run("map missing bool", cleanTest(MapStructValueServiceWithDefault, `{"struct": {"foo": {"hue": 42, "time": "13:00:00 UTC"}}}`, `{"struct": {"foo": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}}`))
	t.Run("map missing text", cleanTest(MapStructValueServiceWithDefault, `{"struct": {"foo": {"hue": 42, "on": true}}}`, `{"struct": {"foo": {"hue": 42, "on": true, "time": "default"}}}`))
	t.Run("map unexpected float", cleanTest(MapStructValueServiceWithDefault, `{"struct": {"foo": {"hue": 42, "on": true, "time": "13:00:00 UTC", "foo":42.13}}}`, `{"struct": {"foo": {"hue": 42, "on": true, "time": "13:00:00 UTC"}}}`))
	t.Run("map wrong name", cleanTest(MapStructValueServiceWithDefault, `{"struct": {"foo": {"foo": 42, "on": true, "time": "13:00:00 UTC"}}}`, `{"struct": {"foo": {"hue": 7, "on": true, "time": "13:00:00 UTC"}}}`))
	t.Run("map 2 elements", cleanTest(MapStructValueServiceWithDefault, `{"struct": {"foo": {"hue": 42, "on": true, "time": "13:00:00 UTC"}, "bar": {"hue": 13}}}`, `{"struct": {"foo": {"hue": 42, "on": true, "time": "13:00:00 UTC"}, "bar": {"hue": 13, "on": true, "time": "default"}}}`))

	t.Run("list exact match", cleanTest(ListStructValueServiceWithDefault, `{"list": [{"hue": 42, "on": true, "time": "13:00:00 UTC"}]}`, `{"list": [{"hue": 42, "on": true, "time": "13:00:00 UTC"}]}`))
	t.Run("list missing str", cleanTest(ListStructValueServiceWithDefault, `{"list": [{"hue": 42, "on": true}]}`, `{"list": [{"hue": 42, "on": true, "time": "default"}]}`))
	t.Run("list missing int", cleanTest(ListStructValueServiceWithDefault, `{"list": [{"on": true, "time": "13:00:00 UTC"}]}`, `{"list": [{"hue": 7, "on": true, "time": "13:00:00 UTC"}]}`))
	t.Run("list missing bool", cleanTest(ListStructValueServiceWithDefault, `{"list": [{"hue": 42, "time": "13:00:00 UTC"}]}`, `{"list": [{"hue": 42, "on": true, "time": "13:00:00 UTC"}]}`))
	t.Run("list extra field", cleanTest(ListStructValueServiceWithDefault, `{"list": [{"foo":"bar", "hue": 42, "on": true, "time": "13:00:00 UTC"}]}`, `{"list": [{"hue": 42, "on": true, "time": "13:00:00 UTC"}]}`))
	t.Run("list 2 elements", cleanTest(ListStructValueServiceWithDefault, `{"list": [{"hue": 42}, {"hue": 13}]}`, `{"list": [{"hue": 42, "on": true, "time": "default"},{"hue": 13, "on": true, "time": "default"}]}`))
	t.Run("list null", cleanTest(ListStructValueServiceWithDefault, `{"list": null}`, `{"list": null}`))
	t.Run("list missing", cleanTest(ListStructValueServiceWithDefault, `{}`, `{"list":[{"hue":7,"on":false,"time":"anotherdefault"}]}`))

	t.Run("array exact match", cleanTest(ArrayValueServiceWithDefault, `{"list": [42, true, "13:00:00 UTC"]}`, `{"list": [42, true, "13:00:00 UTC"]}`))
	t.Run("array 1 missing", cleanTest(ArrayValueServiceWithDefault, `{"list": [42, true]}`, `{"list": [42, true, "default"]}`))
	t.Run("array 2 missing", cleanTest(ArrayValueServiceWithDefault, `{"list": [42]}`, `{"list": [42, true, "default"]}`))
	t.Run("array 3 missing", cleanTest(ArrayValueServiceWithDefault, `{"list": []}`, `{"list": [7, true, "default"]}`))
	t.Run("array null", cleanTest(ArrayValueServiceWithDefault, `{"list": null}`, `{"list": null}`))
	t.Run("array missing", cleanTest(ArrayValueServiceWithDefault, `{}`, `{"list":[7,false,"anotherdefault"]}`))
	t.Run("array extra", cleanTest(ArrayValueServiceWithDefault, `{"list": [42, true, "13:00:00 UTC", "foo"]}`, `{"list": [42, true, "13:00:00 UTC"]}`))
}

func cleanTest(serviceJson string, msgJson string, expectedResultJson string) func(t *testing.T) {
	return func(t *testing.T) {
		service := model.Service{}
		err := json.Unmarshal([]byte(serviceJson), &service)
		if err != nil {
			t.Error(err)
			return
		}

		var expectedResult interface{}
		err = json.Unmarshal([]byte(expectedResultJson), &expectedResult)
		if err != nil {
			t.Error(err)
			return
		}

		var msg map[string]interface{}
		err = json.Unmarshal([]byte(msgJson), &msg)
		if err != nil {
			t.Error(err)
			return
		}

		tempActualResult, err := Clean(msg, service)
		if err != nil {
			t.Error(err)
			return
		}

		//marshal and unmarshal actual result to ensure matching types with generic json parse of expectedResult
		temp, err := json.Marshal(tempActualResult)
		if err != nil {
			t.Error(err)
			return
		}

		var actualResult map[string]interface{}
		err = json.Unmarshal(temp, &actualResult)
		if err != nil {
			t.Error(err)
			return
		}

		if !reflect.DeepEqual(expectedResult, actualResult) {
			t.Error(expectedResultJson, string(temp))
		}
	}

}

const ListStructValueServiceWithDefault = `{
   "local_id":"getStatus",
   "name":"getStatusService",
   "description":"",
   "aspects":[
      {
         "id":"urn:infai:ses:aspect:a7470d73-dde3-41fc-92bd-f16bb28f2da6",
         "name":"Lighting",
         "rdf_type":"https://senergy.infai.org/ontology/Aspect"
      }
   ],
   "protocol_id":"urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
   "inputs":[

   ],
   "outputs":[
      {
         "id":"urn:infai:ses:content:a9f506eb-52ef-4c05-9790-c72aa2975d7f",
         "content_variable":{
            "id":"urn:infai:ses:content-variable:31482062-dc3f-47df-970a-f060a3833e5c",
            "name":"list",
            "type":"https://schema.org/ItemList",
            "sub_content_variables":[
               {
                  "id":"urn:infai:ses:content-variable:31482062-dc3f-47df-970a-f060a3833e5c",
                  "name":"*",
                  "type":"https://schema.org/StructuredValue",
                  "sub_content_variables":[
                     {
                        "id":"urn:infai:ses:content-variable:a003d230-7a27-4263-8880-3a498735a5fd",
                        "name":"hue",
                        "type":"https://schema.org/Integer",
                        "sub_content_variables":null,
                        "characteristic_id":"urn:infai:ses:characteristic:6ec70e99-8c6a-4909-8d5a-7cc12af76b9a",
                        "value":7,
                        "serialization_options":null
                     },
                     {
                        "id":"urn:infai:ses:content-variable:75ac34d8-c7bd-4383-bfb4-aa43ab83d90a",
                        "name":"on",
                        "type":"https://schema.org/Boolean",
                        "sub_content_variables":null,
                        "characteristic_id":"urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
                        "value":true,
                        "serialization_options":null
                     },
                     {
                        "id":"urn:infai:ses:content-variable:29c58291-53b7-4112-9103-7de4c4b04933",
                        "name":"time",
                        "type":"https://schema.org/Text",
                        "sub_content_variables":null,
                        "characteristic_id":"",
                        "value":"default",
                        "serialization_options":null
                     }
                  ],
                  "characteristic_id":"",
                  "value":null,
                  "serialization_options":null
               }
            ],
            "characteristic_id":"",
            "value":[{"hue": 7,"on":false,"time":"anotherdefault"}],
            "serialization_options":null
         },
         "serialization":"json",
         "protocol_segment_id":"urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
      }
   ],
   "functions":[
      {
         "id":"urn:infai:ses:measuring-function:bdb6a7c8-4a3d-4fe0-bab3-ce02e09b5869",
         "name":"getColorFunction",
         "concept_id":"urn:infai:ses:concept:8b1161d5-7878-4dd2-a36c-6f98f6b94bf8",
         "rdf_type":"https://senergy.infai.org/ontology/MeasuringFunction"
      },
      {
         "id":"urn:infai:ses:measuring-function:20d3c1d3-77d7-4181-a9f3-b487add58cd0",
         "name":"getOnOffStateFunction",
         "concept_id":"urn:infai:ses:concept:ebfeabb3-50f0-44bd-b06e-95eb52df484e",
         "rdf_type":"https://senergy.infai.org/ontology/MeasuringFunction"
      }
   ],
   "rdf_type":""
}`

const ArrayValueServiceWithDefault = `{
         "local_id":"getStatus",
         "name":"getStatusService",
         "description":"",
         "aspects":[
            {
               "id":"urn:infai:ses:aspect:a7470d73-dde3-41fc-92bd-f16bb28f2da6",
               "name":"Lighting",
               "rdf_type":"https://senergy.infai.org/ontology/Aspect"
            }
         ],
         "protocol_id":"urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
         "inputs":[

         ],
         "outputs":[
            {
               "id":"urn:infai:ses:content:a9f506eb-52ef-4c05-9790-c72aa2975d7f",
               "content_variable":{
                  "id":"urn:infai:ses:content-variable:31482062-dc3f-47df-970a-f060a3833e5c",
                  "name":"list",
                  "type":"https://schema.org/ItemList",
                  "sub_content_variables":[
                     {
                        "id":"urn:infai:ses:content-variable:a003d230-7a27-4263-8880-3a498735a5fd",
                        "name":"0",
                        "type":"https://schema.org/Integer",
                        "sub_content_variables":null,
                        "characteristic_id":"urn:infai:ses:characteristic:6ec70e99-8c6a-4909-8d5a-7cc12af76b9a",
                        "value":7,
                        "serialization_options":null
                     },
                     {
                        "id":"urn:infai:ses:content-variable:75ac34d8-c7bd-4383-bfb4-aa43ab83d90a",
                        "name":"1",
                        "type":"https://schema.org/Boolean",
                        "sub_content_variables":null,
                        "characteristic_id":"urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
                        "value":true,
                        "serialization_options":null
                     },
                     {
                        "id":"urn:infai:ses:content-variable:29c58291-53b7-4112-9103-7de4c4b04933",
                        "name":"2",
                        "type":"https://schema.org/Text",
                        "sub_content_variables":null,
                        "characteristic_id":"",
                        "value":"default",
                        "serialization_options":null
                     }
                  ],
                  "characteristic_id":"",
                  "value":[7,false,"anotherdefault"],
                  "serialization_options":null
               },
               "serialization":"json",
               "protocol_segment_id":"urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
            }
         ],
         "functions":[
            {
               "id":"urn:infai:ses:measuring-function:bdb6a7c8-4a3d-4fe0-bab3-ce02e09b5869",
               "name":"getColorFunction",
               "concept_id":"urn:infai:ses:concept:8b1161d5-7878-4dd2-a36c-6f98f6b94bf8",
               "rdf_type":"https://senergy.infai.org/ontology/MeasuringFunction"
            },
            {
               "id":"urn:infai:ses:measuring-function:20d3c1d3-77d7-4181-a9f3-b487add58cd0",
               "name":"getOnOffStateFunction",
               "concept_id":"urn:infai:ses:concept:ebfeabb3-50f0-44bd-b06e-95eb52df484e",
               "rdf_type":"https://senergy.infai.org/ontology/MeasuringFunction"
            }
         ],
         "rdf_type":""
      }`

const MapStructValueServiceWithDefault = `{
   "local_id":"getStatus",
   "name":"getStatusService",
   "description":"",
   "aspects":[
      {
         "id":"urn:infai:ses:aspect:a7470d73-dde3-41fc-92bd-f16bb28f2da6",
         "name":"Lighting",
         "rdf_type":"https://senergy.infai.org/ontology/Aspect"
      }
   ],
   "protocol_id":"urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
   "inputs":[

   ],
   "outputs":[
      {
         "id":"urn:infai:ses:content:a9f506eb-52ef-4c05-9790-c72aa2975d7f",
         "content_variable":{
            "id":"urn:infai:ses:content-variable:31482062-dc3f-47df-970a-f060a3833e5c",
            "name":"struct",
            "type":"https://schema.org/StructuredValue",
            "sub_content_variables":[
               {
                  "id":"urn:infai:ses:content-variable:31482062-dc3f-47df-970a-f060a3833e5c",
                  "name":"*",
                  "type":"https://schema.org/StructuredValue",
                  "sub_content_variables":[
                     {
                        "id":"urn:infai:ses:content-variable:a003d230-7a27-4263-8880-3a498735a5fd",
                        "name":"hue",
                        "type":"https://schema.org/Integer",
                        "sub_content_variables":null,
                        "characteristic_id":"urn:infai:ses:characteristic:6ec70e99-8c6a-4909-8d5a-7cc12af76b9a",
                        "value":7,
                        "serialization_options":null
                     },
                     {
                        "id":"urn:infai:ses:content-variable:75ac34d8-c7bd-4383-bfb4-aa43ab83d90a",
                        "name":"on",
                        "type":"https://schema.org/Boolean",
                        "sub_content_variables":null,
                        "characteristic_id":"urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
                        "value":true,
                        "serialization_options":null
                     },
                     {
                        "id":"urn:infai:ses:content-variable:29c58291-53b7-4112-9103-7de4c4b04933",
                        "name":"time",
                        "type":"https://schema.org/Text",
                        "sub_content_variables":null,
                        "characteristic_id":"",
                        "value":"default",
                        "serialization_options":null
                     }
                  ],
                  "characteristic_id":"",
                  "value":null,
                  "serialization_options":null
               }
            ],
            "characteristic_id":"",
            "value":null,
            "serialization_options":null
         },
         "serialization":"json",
         "protocol_segment_id":"urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
      }
   ],
   "functions":[
      {
         "id":"urn:infai:ses:measuring-function:bdb6a7c8-4a3d-4fe0-bab3-ce02e09b5869",
         "name":"getColorFunction",
         "concept_id":"urn:infai:ses:concept:8b1161d5-7878-4dd2-a36c-6f98f6b94bf8",
         "rdf_type":"https://senergy.infai.org/ontology/MeasuringFunction"
      },
      {
         "id":"urn:infai:ses:measuring-function:20d3c1d3-77d7-4181-a9f3-b487add58cd0",
         "name":"getOnOffStateFunction",
         "concept_id":"urn:infai:ses:concept:ebfeabb3-50f0-44bd-b06e-95eb52df484e",
         "rdf_type":"https://senergy.infai.org/ontology/MeasuringFunction"
      }
   ],
   "rdf_type":""
}`

const NestedStructValueServiceWithDefault = `{
   "local_id":"getStatus",
   "name":"getStatusService",
   "description":"",
   "aspects":[
      {
         "id":"urn:infai:ses:aspect:a7470d73-dde3-41fc-92bd-f16bb28f2da6",
         "name":"Lighting",
         "rdf_type":"https://senergy.infai.org/ontology/Aspect"
      }
   ],
   "protocol_id":"urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
   "inputs":[

   ],
   "outputs":[
      {
         "id":"urn:infai:ses:content:a9f506eb-52ef-4c05-9790-c72aa2975d7f",
         "content_variable":{
            "id":"urn:infai:ses:content-variable:31482062-dc3f-47df-970a-f060a3833e5c",
            "name":"struct",
            "type":"https://schema.org/StructuredValue",
            "sub_content_variables":[
               {
                  "id":"urn:infai:ses:content-variable:31482062-dc3f-47df-970a-f060a3833e5c",
                  "name":"struct",
                  "type":"https://schema.org/StructuredValue",
                  "sub_content_variables":[
                     {
                        "id":"urn:infai:ses:content-variable:a003d230-7a27-4263-8880-3a498735a5fd",
                        "name":"hue",
                        "type":"https://schema.org/Integer",
                        "sub_content_variables":null,
                        "characteristic_id":"urn:infai:ses:characteristic:6ec70e99-8c6a-4909-8d5a-7cc12af76b9a",
                        "value":7,
                        "serialization_options":null
                     },
                     {
                        "id":"urn:infai:ses:content-variable:75ac34d8-c7bd-4383-bfb4-aa43ab83d90a",
                        "name":"on",
                        "type":"https://schema.org/Boolean",
                        "sub_content_variables":null,
                        "characteristic_id":"urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
                        "value":true,
                        "serialization_options":null
                     },
                     {
                        "id":"urn:infai:ses:content-variable:29c58291-53b7-4112-9103-7de4c4b04933",
                        "name":"time",
                        "type":"https://schema.org/Text",
                        "sub_content_variables":null,
                        "characteristic_id":"",
                        "value":"default",
                        "serialization_options":null
                     }
                  ],
                  "characteristic_id":"",
                  "value":null,
                  "serialization_options":null
               }
            ],
            "characteristic_id":"",
            "value":null,
            "serialization_options":null
         },
         "serialization":"json",
         "protocol_segment_id":"urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
      }
   ],
   "functions":[
      {
         "id":"urn:infai:ses:measuring-function:bdb6a7c8-4a3d-4fe0-bab3-ce02e09b5869",
         "name":"getColorFunction",
         "concept_id":"urn:infai:ses:concept:8b1161d5-7878-4dd2-a36c-6f98f6b94bf8",
         "rdf_type":"https://senergy.infai.org/ontology/MeasuringFunction"
      },
      {
         "id":"urn:infai:ses:measuring-function:20d3c1d3-77d7-4181-a9f3-b487add58cd0",
         "name":"getOnOffStateFunction",
         "concept_id":"urn:infai:ses:concept:ebfeabb3-50f0-44bd-b06e-95eb52df484e",
         "rdf_type":"https://senergy.infai.org/ontology/MeasuringFunction"
      }
   ],
   "rdf_type":""
}`

const StructValueServiceWithDefault = `{
         "local_id":"getStatus",
         "name":"getStatusService",
         "description":"",
         "aspects":[
            {
               "id":"urn:infai:ses:aspect:a7470d73-dde3-41fc-92bd-f16bb28f2da6",
               "name":"Lighting",
               "rdf_type":"https://senergy.infai.org/ontology/Aspect"
            }
         ],
         "protocol_id":"urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
         "inputs":[

         ],
         "outputs":[
            {
               "id":"urn:infai:ses:content:a9f506eb-52ef-4c05-9790-c72aa2975d7f",
               "content_variable":{
                  "id":"urn:infai:ses:content-variable:31482062-dc3f-47df-970a-f060a3833e5c",
                  "name":"struct",
                  "type":"https://schema.org/StructuredValue",
                  "sub_content_variables":[
                     {
                        "id":"urn:infai:ses:content-variable:a003d230-7a27-4263-8880-3a498735a5fd",
                        "name":"hue",
                        "type":"https://schema.org/Integer",
                        "sub_content_variables":null,
                        "characteristic_id":"urn:infai:ses:characteristic:6ec70e99-8c6a-4909-8d5a-7cc12af76b9a",
                        "value":7,
                        "serialization_options":null
                     },
                     {
                        "id":"urn:infai:ses:content-variable:75ac34d8-c7bd-4383-bfb4-aa43ab83d90a",
                        "name":"on",
                        "type":"https://schema.org/Boolean",
                        "sub_content_variables":null,
                        "characteristic_id":"urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
                        "value":true,
                        "serialization_options":null
                     },
                     {
                        "id":"urn:infai:ses:content-variable:29c58291-53b7-4112-9103-7de4c4b04933",
                        "name":"time",
                        "type":"https://schema.org/Text",
                        "sub_content_variables":null,
                        "characteristic_id":"",
                        "value":"default",
                        "serialization_options":null
                     }
                  ],
                  "characteristic_id":"",
                  "value":null,
                  "serialization_options":null
               },
               "serialization":"json",
               "protocol_segment_id":"urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
            }
         ],
         "functions":[
            {
               "id":"urn:infai:ses:measuring-function:bdb6a7c8-4a3d-4fe0-bab3-ce02e09b5869",
               "name":"getColorFunction",
               "concept_id":"urn:infai:ses:concept:8b1161d5-7878-4dd2-a36c-6f98f6b94bf8",
               "rdf_type":"https://senergy.infai.org/ontology/MeasuringFunction"
            },
            {
               "id":"urn:infai:ses:measuring-function:20d3c1d3-77d7-4181-a9f3-b487add58cd0",
               "name":"getOnOffStateFunction",
               "concept_id":"urn:infai:ses:concept:ebfeabb3-50f0-44bd-b06e-95eb52df484e",
               "rdf_type":"https://senergy.infai.org/ontology/MeasuringFunction"
            }
         ],
         "rdf_type":""
      }`

const SimpleValueServiceWithDefault = `{
         "local_id":"POWER",
         "name":"off",
         "description":"",
         "aspects":[
            {
               "id":"urn:infai:ses:aspect:861227f6-1523-46a7-b8ab-a4e76f0bdd32",
               "name":"Device",
               "rdf_type":"https://senergy.infai.org/ontology/Aspect"
            }
         ],
         "protocol_id":"urn:infai:ses:protocol:c9a06d44-0cd0-465b-b0d9-560d604057a2",
         "outputs":[
            {
               "id":"urn:infai:ses:content:093af08e-5116-4f99-b4c6-3178e4fa8ea1",
               "content_variable":{
                  "id":"urn:infai:ses:content-variable:b3e6c774-161c-4268-94e2-07e87c20b6d4",
                  "name":"state",
                  "type":"https://schema.org/Text",
                  "sub_content_variables":null,
                  "characteristic_id":"",
                  "serialization_options":null,
                  "value": "ON"
               },
               "serialization":"plain-text",
               "protocol_segment_id":"urn:infai:ses:protocol-segment:ffaaf98e-7360-400c-94d4-7775683d38ca"
            }
         ],
         "inputs":[

         ],
         "functions":[
            {
               "id":"urn:infai:ses:controlling-function:2f35150b-9df7-4cad-95bc-165fa00219fd",
               "name":"setOffStateFunction",
               "concept_id":"",
               "rdf_type":"https://senergy.infai.org/ontology/ControllingFunction"
            }
         ],
         "rdf_type":""
      }`

const ListStructValueService = `{
   "local_id":"getStatus",
   "name":"getStatusService",
   "description":"",
   "aspects":[
      {
         "id":"urn:infai:ses:aspect:a7470d73-dde3-41fc-92bd-f16bb28f2da6",
         "name":"Lighting",
         "rdf_type":"https://senergy.infai.org/ontology/Aspect"
      }
   ],
   "protocol_id":"urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
   "inputs":[

   ],
   "outputs":[
      {
         "id":"urn:infai:ses:content:a9f506eb-52ef-4c05-9790-c72aa2975d7f",
         "content_variable":{
            "id":"urn:infai:ses:content-variable:31482062-dc3f-47df-970a-f060a3833e5c",
            "name":"list",
            "type":"https://schema.org/ItemList",
            "sub_content_variables":[
               {
                  "id":"urn:infai:ses:content-variable:31482062-dc3f-47df-970a-f060a3833e5c",
                  "name":"*",
                  "type":"https://schema.org/StructuredValue",
                  "sub_content_variables":[
                     {
                        "id":"urn:infai:ses:content-variable:a003d230-7a27-4263-8880-3a498735a5fd",
                        "name":"hue",
                        "type":"https://schema.org/Integer",
                        "sub_content_variables":null,
                        "characteristic_id":"urn:infai:ses:characteristic:6ec70e99-8c6a-4909-8d5a-7cc12af76b9a",
                        "value":null,
                        "serialization_options":null
                     },
                     {
                        "id":"urn:infai:ses:content-variable:75ac34d8-c7bd-4383-bfb4-aa43ab83d90a",
                        "name":"on",
                        "type":"https://schema.org/Boolean",
                        "sub_content_variables":null,
                        "characteristic_id":"urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
                        "value":null,
                        "serialization_options":null
                     },
                     {
                        "id":"urn:infai:ses:content-variable:29c58291-53b7-4112-9103-7de4c4b04933",
                        "name":"time",
                        "type":"https://schema.org/Text",
                        "sub_content_variables":null,
                        "characteristic_id":"",
                        "value":null,
                        "serialization_options":null
                     }
                  ],
                  "characteristic_id":"",
                  "value":null,
                  "serialization_options":null
               }
            ],
            "characteristic_id":"",
            "value":null,
            "serialization_options":null
         },
         "serialization":"json",
         "protocol_segment_id":"urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
      }
   ],
   "functions":[
      {
         "id":"urn:infai:ses:measuring-function:bdb6a7c8-4a3d-4fe0-bab3-ce02e09b5869",
         "name":"getColorFunction",
         "concept_id":"urn:infai:ses:concept:8b1161d5-7878-4dd2-a36c-6f98f6b94bf8",
         "rdf_type":"https://senergy.infai.org/ontology/MeasuringFunction"
      },
      {
         "id":"urn:infai:ses:measuring-function:20d3c1d3-77d7-4181-a9f3-b487add58cd0",
         "name":"getOnOffStateFunction",
         "concept_id":"urn:infai:ses:concept:ebfeabb3-50f0-44bd-b06e-95eb52df484e",
         "rdf_type":"https://senergy.infai.org/ontology/MeasuringFunction"
      }
   ],
   "rdf_type":""
}`

const ArrayValueService = `{
         "local_id":"getStatus",
         "name":"getStatusService",
         "description":"",
         "aspects":[
            {
               "id":"urn:infai:ses:aspect:a7470d73-dde3-41fc-92bd-f16bb28f2da6",
               "name":"Lighting",
               "rdf_type":"https://senergy.infai.org/ontology/Aspect"
            }
         ],
         "protocol_id":"urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
         "inputs":[

         ],
         "outputs":[
            {
               "id":"urn:infai:ses:content:a9f506eb-52ef-4c05-9790-c72aa2975d7f",
               "content_variable":{
                  "id":"urn:infai:ses:content-variable:31482062-dc3f-47df-970a-f060a3833e5c",
                  "name":"list",
                  "type":"https://schema.org/ItemList",
                  "sub_content_variables":[
                     {
                        "id":"urn:infai:ses:content-variable:a003d230-7a27-4263-8880-3a498735a5fd",
                        "name":"0",
                        "type":"https://schema.org/Integer",
                        "sub_content_variables":null,
                        "characteristic_id":"urn:infai:ses:characteristic:6ec70e99-8c6a-4909-8d5a-7cc12af76b9a",
                        "value":null,
                        "serialization_options":null
                     },
                     {
                        "id":"urn:infai:ses:content-variable:75ac34d8-c7bd-4383-bfb4-aa43ab83d90a",
                        "name":"1",
                        "type":"https://schema.org/Boolean",
                        "sub_content_variables":null,
                        "characteristic_id":"urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
                        "value":null,
                        "serialization_options":null
                     },
                     {
                        "id":"urn:infai:ses:content-variable:29c58291-53b7-4112-9103-7de4c4b04933",
                        "name":"2",
                        "type":"https://schema.org/Text",
                        "sub_content_variables":null,
                        "characteristic_id":"",
                        "value":null,
                        "serialization_options":null
                     }
                  ],
                  "characteristic_id":"",
                  "value":null,
                  "serialization_options":null
               },
               "serialization":"json",
               "protocol_segment_id":"urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
            }
         ],
         "functions":[
            {
               "id":"urn:infai:ses:measuring-function:bdb6a7c8-4a3d-4fe0-bab3-ce02e09b5869",
               "name":"getColorFunction",
               "concept_id":"urn:infai:ses:concept:8b1161d5-7878-4dd2-a36c-6f98f6b94bf8",
               "rdf_type":"https://senergy.infai.org/ontology/MeasuringFunction"
            },
            {
               "id":"urn:infai:ses:measuring-function:20d3c1d3-77d7-4181-a9f3-b487add58cd0",
               "name":"getOnOffStateFunction",
               "concept_id":"urn:infai:ses:concept:ebfeabb3-50f0-44bd-b06e-95eb52df484e",
               "rdf_type":"https://senergy.infai.org/ontology/MeasuringFunction"
            }
         ],
         "rdf_type":""
      }`

const MapStructValueService = `{
   "local_id":"getStatus",
   "name":"getStatusService",
   "description":"",
   "aspects":[
      {
         "id":"urn:infai:ses:aspect:a7470d73-dde3-41fc-92bd-f16bb28f2da6",
         "name":"Lighting",
         "rdf_type":"https://senergy.infai.org/ontology/Aspect"
      }
   ],
   "protocol_id":"urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
   "inputs":[

   ],
   "outputs":[
      {
         "id":"urn:infai:ses:content:a9f506eb-52ef-4c05-9790-c72aa2975d7f",
         "content_variable":{
            "id":"urn:infai:ses:content-variable:31482062-dc3f-47df-970a-f060a3833e5c",
            "name":"struct",
            "type":"https://schema.org/StructuredValue",
            "sub_content_variables":[
               {
                  "id":"urn:infai:ses:content-variable:31482062-dc3f-47df-970a-f060a3833e5c",
                  "name":"*",
                  "type":"https://schema.org/StructuredValue",
                  "sub_content_variables":[
                     {
                        "id":"urn:infai:ses:content-variable:a003d230-7a27-4263-8880-3a498735a5fd",
                        "name":"hue",
                        "type":"https://schema.org/Integer",
                        "sub_content_variables":null,
                        "characteristic_id":"urn:infai:ses:characteristic:6ec70e99-8c6a-4909-8d5a-7cc12af76b9a",
                        "value":null,
                        "serialization_options":null
                     },
                     {
                        "id":"urn:infai:ses:content-variable:75ac34d8-c7bd-4383-bfb4-aa43ab83d90a",
                        "name":"on",
                        "type":"https://schema.org/Boolean",
                        "sub_content_variables":null,
                        "characteristic_id":"urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
                        "value":null,
                        "serialization_options":null
                     },
                     {
                        "id":"urn:infai:ses:content-variable:29c58291-53b7-4112-9103-7de4c4b04933",
                        "name":"time",
                        "type":"https://schema.org/Text",
                        "sub_content_variables":null,
                        "characteristic_id":"",
                        "value":null,
                        "serialization_options":null
                     }
                  ],
                  "characteristic_id":"",
                  "value":null,
                  "serialization_options":null
               }
            ],
            "characteristic_id":"",
            "value":null,
            "serialization_options":null
         },
         "serialization":"json",
         "protocol_segment_id":"urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
      }
   ],
   "functions":[
      {
         "id":"urn:infai:ses:measuring-function:bdb6a7c8-4a3d-4fe0-bab3-ce02e09b5869",
         "name":"getColorFunction",
         "concept_id":"urn:infai:ses:concept:8b1161d5-7878-4dd2-a36c-6f98f6b94bf8",
         "rdf_type":"https://senergy.infai.org/ontology/MeasuringFunction"
      },
      {
         "id":"urn:infai:ses:measuring-function:20d3c1d3-77d7-4181-a9f3-b487add58cd0",
         "name":"getOnOffStateFunction",
         "concept_id":"urn:infai:ses:concept:ebfeabb3-50f0-44bd-b06e-95eb52df484e",
         "rdf_type":"https://senergy.infai.org/ontology/MeasuringFunction"
      }
   ],
   "rdf_type":""
}`

const NestedStructValueService = `{
   "local_id":"getStatus",
   "name":"getStatusService",
   "description":"",
   "aspects":[
      {
         "id":"urn:infai:ses:aspect:a7470d73-dde3-41fc-92bd-f16bb28f2da6",
         "name":"Lighting",
         "rdf_type":"https://senergy.infai.org/ontology/Aspect"
      }
   ],
   "protocol_id":"urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
   "inputs":[

   ],
   "outputs":[
      {
         "id":"urn:infai:ses:content:a9f506eb-52ef-4c05-9790-c72aa2975d7f",
         "content_variable":{
            "id":"urn:infai:ses:content-variable:31482062-dc3f-47df-970a-f060a3833e5c",
            "name":"struct",
            "type":"https://schema.org/StructuredValue",
            "sub_content_variables":[
               {
                  "id":"urn:infai:ses:content-variable:31482062-dc3f-47df-970a-f060a3833e5c",
                  "name":"struct",
                  "type":"https://schema.org/StructuredValue",
                  "sub_content_variables":[
                     {
                        "id":"urn:infai:ses:content-variable:a003d230-7a27-4263-8880-3a498735a5fd",
                        "name":"hue",
                        "type":"https://schema.org/Integer",
                        "sub_content_variables":null,
                        "characteristic_id":"urn:infai:ses:characteristic:6ec70e99-8c6a-4909-8d5a-7cc12af76b9a",
                        "value":null,
                        "serialization_options":null
                     },
                     {
                        "id":"urn:infai:ses:content-variable:75ac34d8-c7bd-4383-bfb4-aa43ab83d90a",
                        "name":"on",
                        "type":"https://schema.org/Boolean",
                        "sub_content_variables":null,
                        "characteristic_id":"urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
                        "value":null,
                        "serialization_options":null
                     },
                     {
                        "id":"urn:infai:ses:content-variable:29c58291-53b7-4112-9103-7de4c4b04933",
                        "name":"time",
                        "type":"https://schema.org/Text",
                        "sub_content_variables":null,
                        "characteristic_id":"",
                        "value":null,
                        "serialization_options":null
                     }
                  ],
                  "characteristic_id":"",
                  "value":null,
                  "serialization_options":null
               }
            ],
            "characteristic_id":"",
            "value":null,
            "serialization_options":null
         },
         "serialization":"json",
         "protocol_segment_id":"urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
      }
   ],
   "functions":[
      {
         "id":"urn:infai:ses:measuring-function:bdb6a7c8-4a3d-4fe0-bab3-ce02e09b5869",
         "name":"getColorFunction",
         "concept_id":"urn:infai:ses:concept:8b1161d5-7878-4dd2-a36c-6f98f6b94bf8",
         "rdf_type":"https://senergy.infai.org/ontology/MeasuringFunction"
      },
      {
         "id":"urn:infai:ses:measuring-function:20d3c1d3-77d7-4181-a9f3-b487add58cd0",
         "name":"getOnOffStateFunction",
         "concept_id":"urn:infai:ses:concept:ebfeabb3-50f0-44bd-b06e-95eb52df484e",
         "rdf_type":"https://senergy.infai.org/ontology/MeasuringFunction"
      }
   ],
   "rdf_type":""
}`

const StructValueService = `{
         "local_id":"getStatus",
         "name":"getStatusService",
         "description":"",
         "aspects":[
            {
               "id":"urn:infai:ses:aspect:a7470d73-dde3-41fc-92bd-f16bb28f2da6",
               "name":"Lighting",
               "rdf_type":"https://senergy.infai.org/ontology/Aspect"
            }
         ],
         "protocol_id":"urn:infai:ses:protocol:f3a63aeb-187e-4dd9-9ef5-d97a6eb6292b",
         "inputs":[

         ],
         "outputs":[
            {
               "id":"urn:infai:ses:content:a9f506eb-52ef-4c05-9790-c72aa2975d7f",
               "content_variable":{
                  "id":"urn:infai:ses:content-variable:31482062-dc3f-47df-970a-f060a3833e5c",
                  "name":"struct",
                  "type":"https://schema.org/StructuredValue",
                  "sub_content_variables":[
                     {
                        "id":"urn:infai:ses:content-variable:a003d230-7a27-4263-8880-3a498735a5fd",
                        "name":"hue",
                        "type":"https://schema.org/Integer",
                        "sub_content_variables":null,
                        "characteristic_id":"urn:infai:ses:characteristic:6ec70e99-8c6a-4909-8d5a-7cc12af76b9a",
                        "value":null,
                        "serialization_options":null
                     },
                     {
                        "id":"urn:infai:ses:content-variable:75ac34d8-c7bd-4383-bfb4-aa43ab83d90a",
                        "name":"on",
                        "type":"https://schema.org/Boolean",
                        "sub_content_variables":null,
                        "characteristic_id":"urn:infai:ses:characteristic:7dc1bb7e-b256-408a-a6f9-044dc60fdcf5",
                        "value":null,
                        "serialization_options":null
                     },
                     {
                        "id":"urn:infai:ses:content-variable:29c58291-53b7-4112-9103-7de4c4b04933",
                        "name":"time",
                        "type":"https://schema.org/Text",
                        "sub_content_variables":null,
                        "characteristic_id":"",
                        "value":null,
                        "serialization_options":null
                     }
                  ],
                  "characteristic_id":"",
                  "value":null,
                  "serialization_options":null
               },
               "serialization":"json",
               "protocol_segment_id":"urn:infai:ses:protocol-segment:0d211842-cef8-41ec-ab6b-9dbc31bc3a65"
            }
         ],
         "functions":[
            {
               "id":"urn:infai:ses:measuring-function:bdb6a7c8-4a3d-4fe0-bab3-ce02e09b5869",
               "name":"getColorFunction",
               "concept_id":"urn:infai:ses:concept:8b1161d5-7878-4dd2-a36c-6f98f6b94bf8",
               "rdf_type":"https://senergy.infai.org/ontology/MeasuringFunction"
            },
            {
               "id":"urn:infai:ses:measuring-function:20d3c1d3-77d7-4181-a9f3-b487add58cd0",
               "name":"getOnOffStateFunction",
               "concept_id":"urn:infai:ses:concept:ebfeabb3-50f0-44bd-b06e-95eb52df484e",
               "rdf_type":"https://senergy.infai.org/ontology/MeasuringFunction"
            }
         ],
         "rdf_type":""
      }`

const SimpleValueService = `{
         "local_id":"POWER",
         "name":"off",
         "description":"",
         "aspects":[
            {
               "id":"urn:infai:ses:aspect:861227f6-1523-46a7-b8ab-a4e76f0bdd32",
               "name":"Device",
               "rdf_type":"https://senergy.infai.org/ontology/Aspect"
            }
         ],
         "protocol_id":"urn:infai:ses:protocol:c9a06d44-0cd0-465b-b0d9-560d604057a2",
         "outputs":[
            {
               "id":"urn:infai:ses:content:093af08e-5116-4f99-b4c6-3178e4fa8ea1",
               "content_variable":{
                  "id":"urn:infai:ses:content-variable:b3e6c774-161c-4268-94e2-07e87c20b6d4",
                  "name":"state",
                  "type":"https://schema.org/Text",
                  "sub_content_variables":null,
                  "characteristic_id":"",
                  "serialization_options":null
               },
               "serialization":"plain-text",
               "protocol_segment_id":"urn:infai:ses:protocol-segment:ffaaf98e-7360-400c-94d4-7775683d38ca"
            }
         ],
         "inputs":[

         ],
         "functions":[
            {
               "id":"urn:infai:ses:controlling-function:2f35150b-9df7-4cad-95bc-165fa00219fd",
               "name":"setOffStateFunction",
               "concept_id":"",
               "rdf_type":"https://senergy.infai.org/ontology/ControllingFunction"
            }
         ],
         "rdf_type":""
      }`
