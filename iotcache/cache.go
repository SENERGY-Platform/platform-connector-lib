package iotcache

import (
	"encoding/json"
	"github.com/SENERGY-Platform/iot-device-repository/lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/iot"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"github.com/bradfitz/gomemcache/memcache"
	"log"
)

type PreparedCache struct {
	iot                  *iot.Iot
	memcached            *memcache.Client
	deviceExpiration     int32
	deviceTypeExpiration int32
	Debug                bool
}

type Cache struct {
	iot                  *iot.Iot
	memcached            *memcache.Client
	deviceExpiration     int32
	deviceTypeExpiration int32
	token                security.JwtToken
	debug                bool
}

func New(iot *iot.Iot, deviceExpiration int32, deviceTypeExpiration int32, memcachedServer ...string) *PreparedCache {
	return &PreparedCache{iot: iot, deviceExpiration: deviceExpiration, deviceTypeExpiration: deviceTypeExpiration, memcached: memcache.New(memcachedServer...)}
}

func (this *PreparedCache) WithToken(token security.JwtToken) *Cache {
	return &Cache{iot: this.iot, deviceExpiration: this.deviceExpiration, deviceTypeExpiration: this.deviceTypeExpiration, debug: this.Debug, memcached: this.memcached, token: token}
}

func (this *Cache) GetDevice(id string) (result model.DeviceInstance, err error) {
	if this.deviceExpiration != 0 {
		result, err = this.getDeviceFromCache(this.token, id)
		if err == nil {
			return
		}
		if err != memcache.ErrCacheMiss {
			log.Println("ERROR: Cache.GetDeviceFromCache() ", err)
		}
	}
	result, err = this.iot.GetDevice(id, this.token)
	if err != nil {
		return
	}
	if this.deviceExpiration != 0 {
		this.saveDeviceToCache(this.token, result)
	}
	return
}

func (this *Cache) DeviceUrlToIotDevice(deviceUrl string) (result []model.DeviceServiceEntity, err error) {
	if this.deviceExpiration != 0 {
		result, err = this.getDeviceUrlToIotDeviceFromCache(this.token, deviceUrl)
		if err == nil {
			return
		}
		if err != memcache.ErrCacheMiss {
			log.Println("ERROR: Cache.DeviceUrlToIotDevice() ", err)
		}
	}
	result, err = this.iot.DeviceUrlToIotDevice(deviceUrl, this.token)
	if err != nil {
		return
	}
	if this.deviceExpiration != 0 {
		this.saveDeviceUrlToIotDeviceToCache(this.token, deviceUrl, result)
	}
	return
}

func (this *Cache) GetDeviceType(id string) (result model.DeviceType, err error) {
	if this.deviceTypeExpiration != 0 {
		result, err = this.getDeviceTypeFromCache(this.token, id)
		if err == nil {
			return
		}
		if err != memcache.ErrCacheMiss {
			log.Println("ERROR: Cache.GetDeviceType() ", err)
		}
	}
	result, err = this.iot.GetDeviceType(id, this.token)
	if err != nil {
		return
	}
	if this.deviceTypeExpiration != 0 {
		this.saveDeviceTypeToCache(this.token, result)
	}
	return
}

func (this *Cache) getDeviceFromCache(token security.JwtToken, id string) (device model.DeviceInstance, err error) {
	pl, err := token.GetPayload()
	if err != nil {
		return device, err
	}
	item, err := this.memcached.Get("device." + pl.UserId + "." + id)
	if err != nil {
		return device, err
	}
	err = json.Unmarshal(item.Value, &device)
	return
}

func (this *Cache) saveDeviceToCache(token security.JwtToken, instance model.DeviceInstance) {
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
	_ = this.memcached.Set(&memcache.Item{Key: "device." + pl.UserId + "." + instance.Id, Value: value, Expiration: this.deviceExpiration})
}

func (this *Cache) getDeviceTypeFromCache(token security.JwtToken, id string) (dt model.DeviceType, err error) {
	item, err := this.memcached.Get("dt." + id)
	if err != nil {
		return dt, err
	}
	if this.debug {
		log.Println("DEBUG: getDeviceTypeFromCache()", "dt."+id, string(item.Value), err)
	}
	err = json.Unmarshal(item.Value, &dt)
	return
}

func (this *Cache) saveDeviceTypeToCache(token security.JwtToken, deviceType model.DeviceType) {
	value, err := json.Marshal(deviceType)
	if err != nil {
		log.Println("WARNING: saveDeviceTypeToCache() unable to marshal instance", err)
		return
	}
	err = this.memcached.Set(&memcache.Item{Key: "dt." + deviceType.Id, Value: value, Expiration: this.deviceTypeExpiration})
	if this.debug {
		log.Println("DEBUG: saveDeviceTypeToCache()", "dt."+deviceType.Id, err)
	}
}

func (this *Cache) getDeviceUrlToIotDeviceFromCache(token security.JwtToken, deviceUrl string) (entities []model.DeviceServiceEntity, err error) {
	pl, err := token.GetPayload()
	if err != nil {
		return entities, err
	}
	item, err := this.memcached.Get("device_url." + pl.UserId + "." + deviceUrl)
	if err != nil {
		return entities, err
	}
	err = json.Unmarshal(item.Value, &entities)
	return
}

func (this *Cache) saveDeviceUrlToIotDeviceToCache(token security.JwtToken, deviceUrl string, entities []model.DeviceServiceEntity) {
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
	_ = this.memcached.Set(&memcache.Item{Key: "device_url." + pl.UserId + "." + deviceUrl, Value: value, Expiration: this.deviceExpiration})
}
