package iot

import (
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"github.com/SENERGY-Platform/platform-connector-lib/statistics"
	"github.com/SENERGY-Platform/service-commons/pkg/cache"
	"github.com/SENERGY-Platform/service-commons/pkg/cache/memcached"
	"github.com/SENERGY-Platform/service-commons/pkg/signal"
	"log"
	"runtime/debug"
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

func NewCache(iot *Iot, deviceExpiration int32, deviceTypeExpiration int32, characteristicExpiration int32, maxIdleConns int, timeout time.Duration, memcachedServer ...string) (*PreparedCache, error) {
	cacheConf := cache.Config{
		ReadCacheHook: func(duration time.Duration) {
			statistics.CacheRead(duration)
		},
		CacheMissHook: func() {
			statistics.CacheMiss()
		},
		CacheInvalidationSignalHooks: map[cache.Signal]cache.ToKey{
			signal.Known.DeviceTypeCacheInvalidation: func(signalValue string) (cacheKey string) {
				return "dt." + signalValue
			},
			signal.Known.CharacteristicCacheInvalidation: func(signalValue string) (cacheKey string) {
				return characteristicCachePrefix + signalValue
			},
		},
	}
	if len(memcachedServer) > 0 {
		cacheConf.L2Provider = memcached.NewProvider(maxIdleConns, timeout, memcachedServer...)
	}

	c, err := cache.New(cacheConf)
	if err != nil {
		return nil, err
	}
	return &PreparedCache{iot: iot, deviceExpiration: deviceExpiration, deviceTypeExpiration: deviceTypeExpiration, characteristicExpiration: characteristicExpiration, cache: c, protocol: map[string]model.Protocol{}}, nil
}

func (this *PreparedCache) WithToken(token security.JwtToken) *Cache {
	return &Cache{parent: this, token: token}
}

func (this *PreparedCache) GetDevice(token security.JwtToken, id string) (result model.Device, err error) {
	if this.deviceExpiration == 0 {
		return this.iot.GetDevice(id, token)
	}
	pl, err := token.GetPayload()
	if err != nil {
		return result, err
	}
	result, err = cache.UseWithValidation(this.cache, "device."+pl.UserId+"."+id, func() (model.Device, error) {
		log.Printf("DEBUG: load device %v from repository\n", id)
		return this.iot.GetDevice(id, token)
	}, time.Duration(this.deviceExpiration)*time.Second, func(device model.Device) error {
		if device.Id == "" {
			return fmt.Errorf("missing device.id")
		}
		if device.DeviceTypeId == "" {
			return fmt.Errorf("missing device.device_type_id")
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	return result, err
}

func (this *PreparedCache) GetDeviceByLocalId(token security.JwtToken, deviceUrl string) (result model.Device, err error) {
	if this.deviceExpiration == 0 {
		return this.iot.GetDeviceByLocalId(deviceUrl, token)
	}
	pl, err := token.GetPayload()
	if err != nil {
		return result, err
	}
	return cache.UseWithValidation(this.cache, "device_url."+pl.UserId+"."+deviceUrl, func() (model.Device, error) {
		log.Printf("DEBUG: load device %v from repository\n", deviceUrl)
		return this.iot.GetDeviceByLocalId(deviceUrl, token)
	}, time.Duration(this.deviceExpiration)*time.Second, func(device model.Device) error {
		if device.Id == "" {
			return fmt.Errorf("missing device.id")
		}
		if device.DeviceTypeId == "" {
			return fmt.Errorf("missing device.device_type_id")
		}
		return nil
	})
}

func (this *PreparedCache) CreateDevice(token security.JwtToken, device model.Device) (result model.Device, err error) {
	result, err = this.iot.CreateDevice(device, token)
	if err == nil {
		this.cacheDevice(token, result)
	}
	return result, err
}

func (this *PreparedCache) cacheDevice(token security.JwtToken, device model.Device) (err error) {
	var pl security.JwtPayload
	pl, err = token.GetPayload()
	if err != nil {
		return err
	}
	log.Printf("DEBUG: cache device %#v", device)
	err = this.cache.Set("device_url."+pl.UserId+"."+device.LocalId, device, time.Duration(this.deviceExpiration)*time.Second)
	if err != nil {
		return err
	}
	err = this.cache.Set("device."+pl.UserId+"."+device.Id, device, time.Duration(this.deviceExpiration)*time.Second)
	if err != nil {
		return err
	}
	return nil
}

func (this *PreparedCache) UpdateDevice(token security.JwtToken, device model.Device) (result model.Device, err error) {
	result, err = this.iot.UpdateDevice(device, token)
	if err == nil {
		this.cacheDevice(token, device)
	}
	return
}

func (this *PreparedCache) CreateDeviceType(token security.JwtToken, deviceType model.DeviceType) (result model.DeviceType, err error) {
	result, err = this.iot.CreateDeviceType(deviceType, token)
	if err == nil {
		this.cache.Set("dt."+result.Id, result, time.Duration(this.deviceTypeExpiration)*time.Second)
	}
	return
}

func (this *PreparedCache) UpdateDeviceType(token security.JwtToken, deviceType model.DeviceType) (result model.DeviceType, err error) {
	result, err = this.iot.UpdateDeviceType(deviceType, token)
	if err == nil {
		this.cache.Set("dt."+result.Id, result, time.Duration(this.deviceTypeExpiration)*time.Second)
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
	if id == "" {
		debug.PrintStack()
		log.Println("ERROR: on GetDeviceType() missing id")
		return result, errors.New("missing id")
	}
	if this.deviceTypeExpiration == 0 {
		return this.iot.GetDeviceType(id, token)
	}
	return cache.UseWithValidation(this.cache, "dt."+id, func() (model.DeviceType, error) {
		return this.iot.GetDeviceType(id, token)
	}, time.Duration(this.deviceTypeExpiration)*time.Second, func(deviceType model.DeviceType) error {
		if deviceType.Id == "" {
			return errors.New("missing device_type.id")
		}
		return nil
	})
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

func (this *PreparedCache) GetCache() *cache.Cache {
	return this.cache
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
