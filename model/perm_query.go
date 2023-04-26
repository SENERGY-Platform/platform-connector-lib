package model

import "github.com/SENERGY-Platform/permission-search/lib/model"

type QueryMessage = model.QueryMessage
type QueryFind = model.QueryFind

type QueryOperationType = model.QueryOperationType

const (
	QueryEqualOperation             = model.QueryEqualOperation
	QueryUnequalOperation           = model.QueryUnequalOperation
	QueryAnyValueInFeatureOperation = model.QueryAnyValueInFeatureOperation
)

type ConditionConfig = model.ConditionConfig

type Selection = model.Selection

type PermSearchDeviceType struct {
	Attributes    []Attribute `json:"attributes,omitempty"`
	Creator       string      `json:"creator,omitempty"`
	Description   string      `json:"description,omitempty"`
	DeviceClassId string      `json:"device_class_id,omitempty"`
	Id            string      `json:"id,omitempty"`
	Name          string      `json:"name,omitempty"`
	Permissions   Permissions `json:"permissions"`
	Protocols     interface{} `json:"protocols,omitempty"`
	Service       interface{} `json:"service,omitempty"`
	Shared        bool        `json:"shared,omitempty"`
}

type Permissions struct {
	A bool
	R bool
	W bool
	X bool
}

type ResourceRights = model.ResourceRights
