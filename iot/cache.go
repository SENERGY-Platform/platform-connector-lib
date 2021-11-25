package iot

import (
	"encoding/json"
	"github.com/SENERGY-Platform/platform-connector-lib/cache"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"github.com/SENERGY-Platform/platform-connector-lib/statistics"
	"log"
	"sync"
	"time"
)

type PreparedCache struct {
	iot                      *Iot
	cache                    *cache.Cache
	deviceExpiration         int32
	deviceTypeExpiration     int32
	characteristicExpiration int32
	Debug                    bool
	protocol                 map[string]model.Protocol
	mux                      sync.RWMutex
}

type Cache struct {
	parent *PreparedCache
	token  security.JwtToken
}

func NewCache(iot *Iot, deviceExpiration int32, deviceTypeExpiration int32, characteristicExpiration int32, maxIdleConns int, timeout time.Duration, memcachedServer ...string) *PreparedCache {
	return &PreparedCache{iot: iot, deviceExpiration: deviceExpiration, deviceTypeExpiration: deviceTypeExpiration, characteristicExpiration: characteristicExpiration, cache: cache.New(maxIdleConns, timeout, memcachedServer...), protocol: map[string]model.Protocol{}}
}

func (this *PreparedCache) SetStatisticsCollector(collector statistics.Interface) *PreparedCache {
	this.cache.SetStatisticsCollector(collector)
	this.iot.SetStatisticsCollector(collector)
	return this
}

func (this *PreparedCache) WithToken(token security.JwtToken) *Cache {
	return &Cache{parent: this, token: token}
}

func (this *PreparedCache) GetDevice(token security.JwtToken, id string) (result model.Device, err error) {
	if this.deviceExpiration != 0 {
		result, err = this.getDeviceFromCache(token, id)
		if err == nil {
			return
		}
		if err != cache.ErrNotFound {
			log.Println("ERROR: Cache.GetDeviceFromCache() ", err)
		}
	}
	result, err = this.iot.GetDevice(id, token)
	if err != nil {
		return
	}
	if this.deviceExpiration != 0 {
		this.saveDeviceToCache(token, result)
	}
	return
}

func (this *PreparedCache) GetDeviceByLocalId(token security.JwtToken, deviceUrl string) (result model.Device, err error) {
	if this.deviceExpiration != 0 {
		result, err = this.getDeviceUrlToIotDeviceFromCache(token, deviceUrl)
		if err == nil {
			return
		}
		if err != cache.ErrNotFound {
			log.Println("ERROR: Cache.DeviceUrlToIotDevice() ", err)
		}
	}
	result, err = this.iot.GetDeviceByLocalId(deviceUrl, token)
	if err != nil {
		return
	}
	if this.deviceExpiration != 0 {
		this.saveDeviceUrlToIotDeviceToCache(token, deviceUrl, result)
	}
	return
}

func (this *PreparedCache) CreateDevice(token security.JwtToken, device model.Device) (result model.Device, err error) {
	result, err = this.iot.CreateDevice(device, token)
	if err == nil {
		this.saveDeviceUrlToIotDeviceToCache(token, device.LocalId, result)
		this.saveDeviceToCache(token, result)
	}
	return
}

func (this *PreparedCache) UpdateDevice(token security.JwtToken, device model.Device) (result model.Device, err error) {
	result, err = this.iot.UpdateDevice(device, token)
	if err == nil {
		this.saveDeviceUrlToIotDeviceToCache(token, device.LocalId, result)
		this.saveDeviceToCache(token, result)
	}
	return
}

func (this *PreparedCache) CreateDeviceType(token security.JwtToken, deviceType model.DeviceType) (result model.DeviceType, err error) {
	result, err = this.iot.CreateDeviceType(deviceType, token)
	if err == nil {
		this.saveDeviceTypeToCache(result)
	}
	return
}

func (this *PreparedCache) UpdateDeviceType(token security.JwtToken, deviceType model.DeviceType) (result model.DeviceType, err error) {
	result, err = this.iot.UpdateDeviceType(deviceType, token)
	if err == nil {
		this.saveDeviceTypeToCache(result)
	}
	return
}

func (this *PreparedCache) EnsureLocalDeviceExistence(token security.JwtToken, device model.Device) (result model.Device, err error) {
	result, err = this.GetDeviceByLocalId(token, device.LocalId)
	if err == security.ErrorNotFound {
		result, err = this.CreateDevice(token, device)
	}
	return
}

func (this *PreparedCache) GetDeviceType(token security.JwtToken, id string) (result model.DeviceType, err error) {
	if this.deviceTypeExpiration != 0 {
		result, err = this.getDeviceTypeFromCache(id)
		if err == nil {
			return
		}
		if err != cache.ErrNotFound {
			log.Println("ERROR: Cache.GetDeviceType() ", err)
		}
	}
	result, err = this.iot.GetDeviceType(id, token)
	if err != nil {
		return
	}
	if this.deviceTypeExpiration != 0 {
		this.saveDeviceTypeToCache(result)
	}
	return
}

func (this *PreparedCache) getDeviceFromCache(token security.JwtToken, id string) (device model.Device, err error) {
	pl, err := token.GetPayload()
	if err != nil {
		return device, err
	}
	item, err := this.cache.Get("device." + pl.UserId + "." + id)
	if err != nil {
		return device, err
	}
	err = json.Unmarshal(item.Value, &device)
	return
}

func (this *PreparedCache) saveDeviceToCache(token security.JwtToken, instance model.Device) {
	pl, err := token.GetPayload()
	if err != nil {
		log.Println("WARNING: saveDeviceToCache() unable to parse token", err)
		return
	}
	value, err := json.Marshal(instance)
	if err != nil {
		log.Println("WARNING: saveDeviceToCache() unable to marshal instance", err)
		return
	}
	this.cache.Set("device."+pl.UserId+"."+instance.Id, value, this.deviceExpiration)
}

func (this *PreparedCache) getDeviceTypeFromCache(id string) (dt model.DeviceType, err error) {
	item, err := this.cache.Get("dt." + id)
	if err != nil {
		return dt, err
	}
	if this.Debug {
		log.Println("DEBUG: getDeviceTypeFromCache()", "dt."+id, string(item.Value), err)
	}
	err = json.Unmarshal(item.Value, &dt)
	return
}

func (this *PreparedCache) saveDeviceTypeToCache(deviceType model.DeviceType) {
	value, err := json.Marshal(deviceType)
	if err != nil {
		log.Println("WARNING: saveDeviceTypeToCache() unable to marshal instance", err)
		return
	}
	this.cache.Set("dt."+deviceType.Id, value, this.deviceTypeExpiration)
}

func (this *PreparedCache) getDeviceUrlToIotDeviceFromCache(token security.JwtToken, deviceUrl string) (entities model.Device, err error) {
	pl, err := token.GetPayload()
	if err != nil {
		return entities, err
	}
	item, err := this.cache.Get("device_url." + pl.UserId + "." + deviceUrl)
	if err != nil {
		return entities, err
	}
	err = json.Unmarshal(item.Value, &entities)
	return
}

func (this *PreparedCache) saveDeviceUrlToIotDeviceToCache(token security.JwtToken, deviceUrl string, entities model.Device) {
	pl, err := token.GetPayload()
	if err != nil {
		log.Println("WARNING: saveDeviceToCache() unable to parse token", err)
		return
	}
	value, err := json.Marshal(entities)
	if err != nil {
		log.Println("WARNING: saveDeviceToCache() unable to marshal entities", err)
		return
	}
	this.cache.Set("device_url."+pl.UserId+"."+deviceUrl, value, this.deviceExpiration)
}

func (this *PreparedCache) GetProtocol(token security.JwtToken, id string) (result model.Protocol, err error) {
	protocol, ok := this.readProtocolFromCache(id)
	if ok {
		return protocol, nil
	}
	protocol, err = this.iot.GetProtocol(id, token)
	if err != nil {
		return protocol, err
	}
	this.writeProtocolToCache(id, protocol)
	return protocol, err
}

func (this *PreparedCache) readProtocolFromCache(id string) (result model.Protocol, ok bool) {
	this.mux.RLock()
	defer this.mux.RUnlock()
	result, ok = this.protocol[id]
	return
}

func (this *PreparedCache) writeProtocolToCache(id string, protocol model.Protocol) {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.protocol[id] = protocol
	return
}

func (this *PreparedCache) Get(key string) (value []byte, err error) {
	item, err := this.cache.Get(key)
	if err != nil {
		return nil, err
	}
	return item.Value, nil
}

func (this *PreparedCache) Set(key string, value []byte, expiration int32) {
	this.cache.Set(key, value, expiration)
}

func (this *PreparedCache) Use(key string, getter func() (interface{}, error), result interface{}, expiration int32) (err error) {
	key = "custom." + key
	value, err := this.Get(key)
	if err == nil {
		err = json.Unmarshal(value, result)
		return
	} else if err != cache.ErrNotFound {
		log.Println("WARNING: err in PreparedCache.Use()", err)
	}
	temp, err := getter()
	if err != nil {
		return err
	}
	value, err = json.Marshal(temp)
	if err != nil {
		return err
	}
	this.Set(key, value, expiration)
	return json.Unmarshal(value, &result)
}

func (this *PreparedCache) InvalidateDeviceTypeCache(deviceTypeId string) {
	this.cache.Remove("dt." + deviceTypeId)
}

func (this *Cache) GetDevice(id string) (result model.Device, err error) {
	return this.parent.GetDevice(this.token, id)
}

func (this *Cache) GetDeviceByLocalId(deviceUrl string) (result model.Device, err error) {
	return this.parent.GetDeviceByLocalId(this.token, deviceUrl)
}

func (this *Cache) CreateDevice(device model.Device) (result model.Device, err error) {
	return this.parent.CreateDevice(this.token, device)
}

func (this *Cache) EnsureLocalDeviceExistence(device model.Device) (result model.Device, err error) {
	return this.parent.EnsureLocalDeviceExistence(this.token, device)
}

func (this *Cache) GetDeviceType(id string) (result model.DeviceType, err error) {
	return this.parent.GetDeviceType(this.token, id)
}

func (this *Cache) GetProtocol(id string) (result model.Protocol, err error) {
	return this.parent.GetProtocol(this.token, id)
}
