package xml

import (
	"github.com/SENERGY-Platform/platform-connector-lib/marshalling/base"
)

type Marshaller struct {
}

const Format = "xml"

func init() {
	base.Register(Format, Marshaller{})
}
