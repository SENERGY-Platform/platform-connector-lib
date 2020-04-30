package iot

import (
	"fmt"
	"net/http"
)

func NewLogger(handler http.Handler, ctrl *Controller) *LoggerMiddleWare {
	return &LoggerMiddleWare{handler: handler, ctrl: ctrl}
}

type LoggerMiddleWare struct {
	handler http.Handler
	ctrl    *Controller
}

func (this *LoggerMiddleWare) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	this.log(r)
	if this.handler != nil {
		this.handler.ServeHTTP(w, r)
	} else {
		http.Error(w, "Forbidden", 403)
	}
}

func (this *LoggerMiddleWare) log(request *http.Request) {
	this.ctrl.calls = append(this.ctrl.calls, fmt.Sprintf("[%v] %v \n", request.Method, request.URL))
}
