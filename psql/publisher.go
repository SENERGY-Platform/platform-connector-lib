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
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
	"log"
	"strings"
	"sync"
	"time"
)

type Publisher struct {
	db    *pgxpool.Pool
	debug bool
}

var ConnectionTimeout = 10 * time.Second

func New(postgresHost string, postgresPort int, postgresUser string, postgresPw string, postgresDb string, debugLog bool, wg *sync.WaitGroup, basectx context.Context) (*Publisher, error) {
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", postgresHost,
		postgresPort, postgresUser, postgresPw, postgresDb)

	config, err := pgxpool.ParseConfig(psqlconn)
	if err != nil {
		return nil, err
	}
	config.MaxConns = 50

	ctx, cancel := context.WithCancel(basectx)
	timeout, timeoutcancel := context.WithTimeout(basectx, ConnectionTimeout)
	defer timeoutcancel()
	go func() {
		<-timeout.Done()
		if timeout.Err() != context.Canceled {
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
		db:    db,
		debug: debugLog,
	}, nil
}

var SlowProducerTimeout time.Duration = 2 * time.Second

func (publisher *Publisher) Publish(envelope model.Envelope) (err error) {
	start := time.Now()
	jsonMsg, ok := envelope.Value.(map[string]interface{})
	if !ok {
		return errors.New("envelope.Value is no map[string]interface{}")
	}
	m := flatten(jsonMsg)

	shortDeviceId, err := ShortenId(envelope.DeviceId)
	if err != nil {
		return err
	}
	shortServiceId, err := ShortenId(envelope.ServiceId)
	if err != nil {
		return err
	}
	table := "device:" + shortDeviceId + "_" + "service:" + shortServiceId

	query := "INSERT INTO \"" + table + "\"("

	fields := make([]string, len(m)+1)
	values := make([]string, len(m)+1)

	fields[0] = "\"time\""
	values[0] = "'" + time.Now().Format(time.RFC3339Nano) + "'"

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

	if publisher.debug {
		log.Println("QUERY:", query)
	}

	_, err = publisher.db.Exec(context.Background(), query)
	if publisher.debug {
		log.Println("Postgres publishing took ", time.Since(start))
	}
	if SlowProducerTimeout > 0 && time.Since(start) >= SlowProducerTimeout {
		log.Println("WARNING: finished slow timescale publisher call", time.Since(start), envelope.DeviceId, envelope.ServiceId)
	}
	return err
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
