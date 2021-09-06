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
	"database/sql"
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	_ "github.com/lib/pq"
	"log"
	"strings"
	"sync"
	"time"
)

type Publisher struct {
	db    *sql.DB
	debug bool
}

func New(postgresHost string, postgresPort int, postgresUser string, postgresPw string, postgresDb string, debug bool, wg *sync.WaitGroup, ctx context.Context) (*Publisher, error) {
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", postgresHost,
		postgresPort, postgresUser, postgresPw, postgresDb)

	// open database
	db, err := sql.Open("postgres", psqlconn)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	wg.Add(1)
	go func() {
		<-ctx.Done()
		_ = db.Close()
		wg.Done()
	}()
	return &Publisher{
		db:    db,
		debug: debug,
	}, nil
}

func (publisher *Publisher) Publish(envelope model.Envelope) error {
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
		values[i] = fmt.Sprintf("%v", v)
		i++
	}

	query += strings.Join(fields, ",") + ") VALUES (" + strings.Join(values, ",") + ");"

	if publisher.debug {
		log.Println("QUERY:", query)
	}

	_, err = publisher.db.Exec(query)
	if publisher.debug {
		log.Println("Postgres publishing took ", time.Since(start))
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
