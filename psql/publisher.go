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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
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
	cacheMux                         sync.RWMutex
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
			logger.Error("psql publisher connection timeout", "err", timeout.Err())
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

	timeStr, timeErr, notify := publisher.getTimeString(m, service)
	if timeErr != nil {
		return timeErr, notify
	}

	query, args := buildInsertQuery(table, timeStr, m)

	publisher.logger.Debug("psql request", "query", query, "args", args)

	_, err = publisher.db.Exec(context.Background(), query, args...)

	publisher.logger.Debug("psql response", "err", err, "duration", time.Since(start))
	if SlowProducerTimeout > 0 && time.Since(start) >= SlowProducerTimeout {
		publisher.logger.Warn("finished slow timescale publisher call", "duration", time.Since(start), "envelope", envelope, "deviceId", envelope.DeviceId, "serviceId", envelope.ServiceId)
	}
	return err, false
}

// getTimeString determines the value of the time column. If the service carries a
// senergy/time_path attribute, the referenced message value is used, otherwise the
// time of the write.
func (publisher *Publisher) getTimeString(m map[string]interface{}, service model.Service) (timeStr string, err error, notifyUser bool) {
	for _, attr := range service.Attributes {
		if attr.Key != timeAttributeKey || len(attr.Value) == 0 {
			continue
		}
		characteristicId, err := publisher.getTimeCharacteristicId(service, attr.Value)
		if err != nil {
			return "", err, true
		}
		timeVal, ok := m[attr.Value]
		if !ok {
			return "", errors.New("Can't find value with path " + attr.Value + " in message"), true
		}
		timeVal, err = publisher.conv.Cast(timeVal, characteristicId, characteristics.UnixNanoSeconds)
		if err != nil {
			return "", err, true
		}
		nanoseconds, err := toNanoseconds(timeVal)
		if err != nil {
			return "", fmt.Errorf("value with path %v: %w", attr.Value, err), true
		}
		return time.Unix(0, nanoseconds).UTC().Format(time.RFC3339Nano), nil, false
	}
	return time.Now().UTC().Format(time.RFC3339Nano), nil, false
}

// getTimeCharacteristicId resolves the characteristic of the content variable the
// senergy/time_path attribute points to. The cache is shared by the publish
// goroutines of all events and therefore guarded.
func (publisher *Publisher) getTimeCharacteristicId(service model.Service, path string) (characteristicId string, err error) {
	publisher.cacheMux.RLock()
	cached, ok := publisher.serviceIdTimeCharacteristicCache[service.Id]
	publisher.cacheMux.RUnlock()
	if ok && time.Since(cached.Timestamp) <= cacheDuration {
		return cached.CharacteristicId, nil
	}
	pathParts := strings.Split(path, ".")
	for _, output := range service.Outputs {
		if output.ContentVariable.Name != pathParts[0] {
			continue
		}
		timeContentVariable := getDeepContentVariable(output.ContentVariable, pathParts[1:])
		if timeContentVariable == nil {
			return "", errors.New("Can't find content variable with path " + path)
		}
		cached = characteristicIdTimestamp{
			CharacteristicId: timeContentVariable.CharacteristicId,
			Timestamp:        time.Now(),
		}
		publisher.cacheMux.Lock()
		publisher.serviceIdTimeCharacteristicCache[service.Id] = cached
		publisher.cacheMux.Unlock()
	}
	return cached.CharacteristicId, nil
}

// toNanoseconds interprets the result of a cast to characteristics.UnixNanoSeconds.
// converter.Cast returns the input unchanged if source and target characteristic are
// equal, so the type is the one the message was decoded to and not the one the casts
// to UnixNanoSeconds produce. A float64 above 2^53 has already lost precision at that
// point; nanosecond timestamps are of that size.
func toNanoseconds(value interface{}) (nanoseconds int64, err error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case float64:
		if v >= float64(math.MaxInt64) || v < float64(math.MinInt64) {
			return 0, fmt.Errorf("timestamp %v is out of range for unix nanoseconds", v)
		}
		return int64(v), nil
	case float32:
		return toNanoseconds(float64(v))
	case json.Number:
		return v.Int64()
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("unable to interpret %T as unix nanoseconds", value)
	}
}

// buildInsertQuery returns the query and the arguments belonging to it. The message
// values are passed as parameters, so that they need no quoting and cannot terminate
// the statement. The time is not a parameter because it is the output of
// time.Time.Format and pgx does not encode a string for a timestamptz column.
func buildInsertQuery(table string, timeStr string, m map[string]interface{}) (query string, args []interface{}) {
	fields := make([]string, len(m)+1)
	values := make([]string, len(m)+1)
	args = make([]interface{}, 0, len(m))

	fields[0] = quoteIdentifier("time")
	values[0] = "'" + timeStr + "'"

	i := 1
	for k, v := range m {
		fields[i] = quoteIdentifier(k)
		values[i] = "$" + strconv.Itoa(i)
		args = append(args, v)
		i++
	}

	return "INSERT INTO " + quoteIdentifier(table) + "(" + strings.Join(fields, ",") + ") VALUES (" + strings.Join(values, ",") + ");", args
}

// quoteIdentifier quotes a table or column name. Identifiers cannot be passed as
// parameters, and the names originate from device and service ids and from the field
// names of the message, so a quote in a name has to be escaped.
func quoteIdentifier(name string) string {
	return "\"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""
}

// flatten keeps the values as they were decoded; they are passed to the query as
// parameters, so that a value may still be read and cast before the query is built
func flatten(m map[string]interface{}) (values map[string]interface{}) {
	values = make(map[string]interface{})
	for k, v := range m {
		switch child := v.(type) {
		case map[string]interface{}:
			nm := flatten(child)
			for nk, nv := range nm {
				values[k+"."+nk] = nv
			}
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
