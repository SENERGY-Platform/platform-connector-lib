/*
 * Copyright 2020 InfAI (CC SES)
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

package httpcommand

import (
	"context"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

func StartConsumer(ctx context.Context, port string, listener func(msg []byte) error) error {
	router := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost && strings.TrimPrefix(request.URL.Path, "/") == "commands" {
			msg, err := io.ReadAll(request.Body)
			if err != nil {
				log.Println("ERROR:", err)
				http.Error(writer, err.Error(), http.StatusBadRequest)
				return
			}
			err = listener(msg)
			if err != nil {
				log.Println("ERROR:", err)
				http.Error(writer, err.Error(), http.StatusBadRequest)
				return
			}
			writer.WriteHeader(http.StatusOK)
			return
		} else {
			http.Error(writer, "unknown endpoint", http.StatusNotFound)
			return
		}
	})
	corsHandler := NewCors(router)
	logger := NewLogger(corsHandler)
	server := &http.Server{Addr: ":" + port, Handler: logger, WriteTimeout: 10 * time.Second, ReadTimeout: 10 * time.Second, ReadHeaderTimeout: 2 * time.Second}
	go func() {
		log.Println("Listening on ", server.Addr)
		if err := server.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				log.Println("ERROR: http command consumer server error", err)
				log.Fatal(err)
			} else {
				log.Println("closing http command consumer server")
			}
		}
	}()
	go func() {
		<-ctx.Done()
		log.Println("http command consumer shutdown", server.Shutdown(context.Background()))
	}()
	return nil
}
