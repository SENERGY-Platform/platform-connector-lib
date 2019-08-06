package xml

import (
	"mapping/marshalling/base"
)

type Marshaller struct {
}

const Format = "xml"

func init() {
	base.Register(Format, Marshaller{})
}
