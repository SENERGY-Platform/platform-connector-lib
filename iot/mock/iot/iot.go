package iot

import (
	"context"
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/google/uuid"
	"github.com/julienschmidt/httprouter"
	"log"
	"net/http/httptest"
	"runtime/debug"
	"sync"
)

func Mock(ctx context.Context) (ctrl *Controller, url string, err error) {
	ctrl = &Controller{
		mux:              sync.Mutex{},
		devices:          map[string]model.Device{},
		characteristics:  map[string]model.Characteristic{},
		devicesByLocalId: map[string]string{},
		deviceTypes:      map[string]model.DeviceType{},
		hubs:             map[string]model.Hub{},
		protocols:        map[string]model.Protocol{},
		calls:            []string{},
	}
	router, err := getRouter(ctrl)
	if err != nil {
		return ctrl, "", err
	}

	server := httptest.NewServer(NewLogger(router, ctrl))
	go func() {
		<-ctx.Done()
		server.Close()
	}()
	return ctrl, server.URL, nil
}

func getRouter(controller *Controller) (router *httprouter.Router, err error) {
	defer func() {
		if r := recover(); r != nil && err == nil {
			log.Printf("%s: %s", r, debug.Stack())
			err = errors.New(fmt.Sprint("Recovered Error: ", r))
		}
	}()
	router = httprouter.New()
	DevicesEndpoints(controller, router)
	LocalDevicesEndpoints(controller, router)
	DeviceTypesEndpoints(controller, router)
	HubsEndpoints(controller, router)
	ProtocolsEndpoints(controller, router)
	CharacteristicsEndpoints(controller, router)
	return
}

type Controller struct {
	mux              sync.Mutex
	devices          map[string]model.Device
	characteristics  map[string]model.Characteristic
	devicesByLocalId map[string]string
	deviceTypes      map[string]model.DeviceType
	hubs             map[string]model.Hub
	protocols        map[string]model.Protocol
	calls            []string
}

func (this *Controller) GetCalls() (result []string) {
	return this.calls
}

func (this *Controller) ReadDevice(id string) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	var exists bool
	result, exists = this.devices[id]
	if exists {
		return result, nil, 200
	} else {
		return nil, errors.New("404"), 404
	}
}

func (this *Controller) PublishDeviceCreate(device model.Device) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	device.Id = uuid.NewString()
	this.devices[device.Id] = device
	this.devicesByLocalId[device.LocalId] = device.Id
	return device, nil, 200
}

func (this *Controller) PublishDeviceUpdate(id string, device model.Device) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.devices[id] = device
	this.devicesByLocalId[id] = device.LocalId
	return device, nil, 200
}

func (this *Controller) PublishDeviceDelete(id string) (err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	delete(this.devices, id)
	delete(this.devicesByLocalId, id)
	return nil, 200
}

func (this *Controller) ReadHub(id string) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	var exists bool
	result, exists = this.hubs[id]
	if exists {
		return result, nil, 200
	} else {
		return nil, errors.New("404"), 404
	}
}

func (this *Controller) PublishHubCreate(hub model.Hub) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	hub.Id = uuid.NewString()
	this.hubs[hub.Id] = hub
	return hub, nil, 200
}

func (this *Controller) PublishHubUpdate(id string, hub model.Hub) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.hubs[id] = hub
	return hub, nil, 200
}

func (this *Controller) PublishHubDelete(id string) (err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	delete(this.hubs, id)
	return nil, 200
}

func (this *Controller) ReadDeviceType(id string) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	var exists bool
	result, exists = this.deviceTypes[id]
	if exists {
		return result, nil, 200
	} else {
		return nil, errors.New("404"), 404
	}
}

func (this *Controller) PublishDeviceTypeCreate(devicetype model.DeviceType) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	devicetype.Id = uuid.NewString()
	for i, service := range devicetype.Services {
		service.Id = uuid.NewString()
		devicetype.Services[i] = service
	}
	this.deviceTypes[devicetype.Id] = devicetype
	return devicetype, nil, 200
}

func (this *Controller) PublishDeviceTypeUpdate(id string, devicetype model.DeviceType) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.deviceTypes[id] = devicetype
	return devicetype, nil, 200
}

func (this *Controller) PublishDeviceTypeDelete(id string) (err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	delete(this.deviceTypes, id)
	return nil, 200
}

func (this *Controller) DeviceLocalIdToId(id string) (result string, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	var exists bool
	result, exists = this.devicesByLocalId[id]
	if exists {
		return result, nil, 200
	} else {
		return "", errors.New("404"), 404
	}
}

func (this *Controller) ReadProtocol(id string) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	var exists bool
	result, exists = this.protocols[id]
	if exists {
		return result, nil, 200
	} else {
		return nil, errors.New("404"), 404
	}
}

func (this *Controller) PublishProtocolCreate(protocol model.Protocol) (result model.Protocol, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	protocol.Id = uuid.NewString()
	this.protocols[protocol.Id] = protocol
	return protocol, nil, 200
}

func (this *Controller) PublishProtocolUpdate(id string, protocol model.Protocol) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.protocols[id] = protocol
	return protocol, nil, 200
}

func (this *Controller) PublishProtocolDelete(id string) (err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	delete(this.protocols, id)
	return nil, 200
}

func (this *Controller) ReadCharacteristic(id string) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	var exists bool
	result, exists = this.characteristics[id]
	if exists {
		return result, nil, 200
	} else {
		return nil, errors.New("404"), 404
	}
}

func (this *Controller) PublishCharacteristicCreate(characteristic model.Characteristic) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	characteristic.Id = uuid.NewString()
	this.characteristics[characteristic.Id] = characteristic
	return characteristic, nil, 200
}

func (this *Controller) PublishCharacteristicUpdate(id string, characteristic model.Characteristic) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.characteristics[id] = characteristic
	return characteristic, nil, 200
}

func (this *Controller) PublishCharacteristicDelete(id string) (err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	delete(this.characteristics, id)
	return nil, 200
}
