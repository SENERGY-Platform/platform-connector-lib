/*
 * Copyright 2021 InfAI (CC SES)
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

package psql

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/SENERGY-Platform/converter/lib/converter"
	"github.com/SENERGY-Platform/converter/lib/converter/characteristics"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
)

type Publisher struct {
	db                               *pgxpool.Pool
	serviceIdTimeCharacteristicCache map[string]characteristicIdTimestamp
	conv                             *converter.Converter
	logger                           *slog.Logger
}

type characteristicIdTimestamp struct {
	CharacteristicId string
	Timestamp        time.Time
}

var ConnectionTimeout = 10 * time.Second
var timeAttributeKey = "senergy/time_path"
var cacheDuration = 5 * time.Minute

func New(postgresHost string, postgresPort int, postgresUser string, postgresPw string, postgresDb string, logger *slog.Logger, wg *sync.WaitGroup, basectx context.Context) (*Publisher, error) {
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", postgresHost,
		postgresPort, postgresUser, postgresPw, postgresDb)

	config, err := pgxpool.ParseConfig(psqlconn)
	if err != nil {
		return nil, err
	}
	config.MaxConns = 50

	conv, err := converter.New()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(basectx)
	timeout, timeoutcancel := context.WithTimeout(basectx, ConnectionTimeout)
	defer timeoutcancel()
	go func() {
		<-timeout.Done()
		if !errors.Is(timeout.Err(), context.Canceled) {
			log.Println("ERROR: psql publisher connection timeout")
			cancel()
		}
	}()

	db, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	err = db.Ping(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}

	wg.Add(1)
	go func() {
		<-ctx.Done()
		db.Close()
		wg.Done()
	}()
	return &Publisher{
		db:                               db,
		logger:                           logger,
		serviceIdTimeCharacteristicCache: map[string]characteristicIdTimestamp{},
		conv:                             conv,
	}, nil
}

var SlowProducerTimeout time.Duration = 2 * time.Second

func (publisher *Publisher) Publish(envelope model.Envelope, service model.Service) (err error, notifyUsers bool) {
	start := time.Now()
	m := flatten(envelope.Value)

	shortDeviceId, err := ShortenId(envelope.DeviceId)
	if err != nil {
		return err, false
	}
	shortServiceId, err := ShortenId(envelope.ServiceId)
	if err != nil {
		return err, false
	}
	table := "device:" + shortDeviceId + "_" + "service:" + shortServiceId

	query := "INSERT INTO \"" + table + "\"("

	fields := make([]string, len(m)+1)
	values := make([]string, len(m)+1)

	fields[0] = "\"time\""

	timeStr := time.Now().UTC().Format(time.RFC3339Nano)
	for _, attr := range service.Attributes {
		if attr.Key == timeAttributeKey && len(attr.Value) > 0 {
			cachedCharacteristicIdTimestamp, ok := publisher.serviceIdTimeCharacteristicCache[service.Id]
			if !ok || time.Now().Sub(cachedCharacteristicIdTimestamp.Timestamp) > cacheDuration {
				pathParts := strings.Split(attr.Value, ".")
				for _, output := range service.Outputs {
					if output.ContentVariable.Name != pathParts[0] {
						continue
					}
					timeContentVariable := getDeepContentVariable(output.ContentVariable, pathParts[1:])
					if timeContentVariable == nil {
						return errors.New("Can't find content variable with path " + attr.Value), true
					}
					cachedCharacteristicIdTimestamp = characteristicIdTimestamp{
						CharacteristicId: timeContentVariable.CharacteristicId,
						Timestamp:        time.Now(),
					}
					publisher.serviceIdTimeCharacteristicCache[service.Id] = cachedCharacteristicIdTimestamp
				}
			}
			timeVal, ok := m[attr.Value]
			if !ok {
				return errors.New("Can't find value with path " + attr.Value + " in message"), true
			}
			timeVal, err = publisher.conv.Cast(timeVal, cachedCharacteristicIdTimestamp.CharacteristicId, characteristics.UnixNanoSeconds)
			if err != nil {
				return err, true
			}
			timeStr = time.Unix(0, timeVal.(int64)).UTC().Format(time.RFC3339Nano)
			break
		}
	}

	values[0] = "'" + timeStr + "'"

	i := 1
	for k, v := range m {
		fields[i] = "\"" + k + "\""
		if v != nil {
			values[i] = fmt.Sprintf("%v", v)
		} else {
			values[i] = "NULL"
		}
		i++
	}

	query += strings.Join(fields, ",") + ") VALUES (" + strings.Join(values, ",") + ");"

	publisher.logger.Debug("psql request", "query", query)

	_, err = publisher.db.Exec(context.Background(), query)

	publisher.logger.Debug("psql response", "err", err, "duration", time.Since(start))
	if SlowProducerTimeout > 0 && time.Since(start) >= SlowProducerTimeout {
		log.Println("WARNING: finished slow timescale publisher call", time.Since(start), envelope.DeviceId, envelope.ServiceId)
	}
	return err, false
}

func flatten(m map[string]interface{}) (values map[string]interface{}) {
	values = make(map[string]interface{})
	for k, v := range m {
		switch child := v.(type) {
		case map[string]interface{}:
			nm := flatten(child)
			for nk, nv := range nm {
				values[k+"."+nk] = nv
			}
		case string:
			values[k] = "'" + v.(string) + "'"
		default:
			values[k] = v
		}
	}
	return values
}

func getDeepContentVariable(root model.ContentVariable, path []string) *model.ContentVariable {
	if len(path) == 0 {
		return &root
	}
	if root.SubContentVariables == nil {
		return nil
	}
	for _, sub := range root.SubContentVariables {
		if sub.Name == path[0] {
			return getDeepContentVariable(sub, path[1:])
		}
	}
	return nil
}
