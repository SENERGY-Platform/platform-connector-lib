package model

type QueryMessage struct {
	Resource      string         `json:"resource"`
	Find          *QueryFind     `json:"find,omitempty"`
	ListIds       *QueryListIds  `json:"list_ids,omitempty"`
	CheckIds      *QueryCheckIds `json:"check_ids,omitempty"`
	TermAggregate *string        `json:"term_aggregate,omitempty"`
}
type QueryFind struct {
	QueryListCommons
	Search string     `json:"search,omitempty"`
	Filter *Selection `json:"filter,omitempty"`
}

type QueryListIds struct {
	QueryListCommons
	Ids []string `json:"ids,omitempty"`
}

type QueryCheckIds struct {
	Ids    []string `json:"ids,omitempty"`
	Rights string   `json:"rights,omitempty"`
}

type QueryListCommons struct {
	Limit    int    `json:"limit,omitempty"`
	Offset   int    `json:"offset,omitempty"`
	Rights   string `json:"rights,omitempty"`
	SortBy   string `json:"sort_by,omitempty"`
	SortDesc bool   `json:"sort_desc,omitempty"`
}

type QueryOperationType string

const (
	QueryEqualOperation             QueryOperationType = "=="
	QueryUnequalOperation           QueryOperationType = "!="
	QueryAnyValueInFeatureOperation QueryOperationType = "any_value_in_feature"
)

type ConditionConfig struct {
	Feature   string             `json:"feature,omitempty"`
	Operation QueryOperationType `json:"operation,omitempty"`
	Value     interface{}        `json:"value,omitempty"`
	Ref       string             `json:"ref,omitempty"`
}

type Selection struct {
	And       []Selection      `json:"and,omitempty"`
	Or        []Selection      `json:"or,omitempty"`
	Not       *Selection       `json:"not,omitempty"`
	Condition *ConditionConfig `json:"condition,omitempty"`
}

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
