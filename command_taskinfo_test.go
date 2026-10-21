/*
 * Copyright 2026 InfAI (CC SES)
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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
)

// failingAccessSecurity lets HandleCommandResponse skip the response-as-event
// path, which needs a token, so the test only covers the response itself.
type failingAccessSecurity struct {
	Security
}

func (failingAccessSecurity) Access() (security.JwtToken, error) {
	return "", errors.New("no token in test")
}

func handleCommandResponseAndCapture(t *testing.T, request string) map[string]interface{} {
	t.Helper()
	received := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- body
	}))
	defer server.Close()

	msg := model.ProtocolMsg{}
	if err := json.Unmarshal([]byte(request), &msg); err != nil {
		t.Fatal(err)
	}
	msg.Metadata.ResponseTo = server.URL

	connector := &Connector{security: failingAccessSecurity{}}
	if err := connector.HandleCommandResponse(msg, CommandResponseMsg{"data": "42"}, Sync); err != nil {
		t.Fatal(err)
	}

	result := map[string]interface{}{}
	if err := json.Unmarshal(<-received, &result); err != nil {
		t.Fatal(err)
	}
	taskInfo, ok := result["task_info"].(map[string]interface{})
	if !ok {
		t.Fatalf("response has no task_info: %v", result)
	}
	return taskInfo
}

func TestHandleCommandResponseMirrorsDeploymentId(t *testing.T) {
	taskInfo := handleCommandResponseAndCapture(t, `{
		"request": {"input": {"data": "1"}},
		"task_info": {
			"worker_id": "w", "task_id": "t", "process_instance_id": "pi",
			"deployment_id": "dep-1", "process_definition_id": "dep-1",
			"completion_strategy": "pessimistic", "tenant_id": "tenant"
		}
	}`)
	if taskInfo["deployment_id"] != "dep-1" {
		t.Errorf("deployment_id = %v, want dep-1", taskInfo["deployment_id"])
	}
	if taskInfo["process_definition_id"] != "dep-1" {
		t.Errorf("process_definition_id = %v, want dep-1", taskInfo["process_definition_id"])
	}
	if taskInfo["task_id"] != "t" || taskInfo["tenant_id"] != "tenant" {
		t.Errorf("task_info not mirrored: %v", taskInfo)
	}
}

func TestHandleCommandResponseWithoutDeploymentId(t *testing.T) {
	taskInfo := handleCommandResponseAndCapture(t, `{
		"request": {"input": {"data": "1"}},
		"task_info": {
			"worker_id": "w", "task_id": "t", "process_instance_id": "pi",
			"process_definition_id": "pd-1", "completion_strategy": "pessimistic"
		}
	}`)
	if taskInfo["process_definition_id"] != "pd-1" {
		t.Errorf("process_definition_id = %v, want pd-1", taskInfo["process_definition_id"])
	}
	if taskInfo["deployment_id"] != "" {
		t.Errorf("deployment_id = %v, want empty", taskInfo["deployment_id"])
	}
	if taskInfo["task_id"] != "t" {
		t.Errorf("task_id = %v, want t", taskInfo["task_id"])
	}
}
