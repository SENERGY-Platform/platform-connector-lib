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
	"errors"
	"io"
	"log"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

func StartConsumer(ctx context.Context, slogger *slog.Logger, port string, listener func(msg []byte) error) error {
	router := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost && strings.TrimPrefix(request.URL.Path, "/") == "commands" {
			msg, err := io.ReadAll(request.Body)
			if err != nil {
				slogger.Error("unable to read http command message", "error", err)
				http.Error(writer, err.Error(), http.StatusBadRequest)
				return
			}
			err = listener(msg)
			if err != nil {
				slogger.Error("unable to handle http command message", "error", err)
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
		slogger.Info("starting http command consumer server", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				slogger.Error("http command consumer server error", "error", err)
				log.Fatal(err)
			} else {
				slogger.Info("http command consumer server closed")
			}
		}
	}()
	go func() {
		<-ctx.Done()
		slogger.Info("shutting down http command consumer server", "result", server.Shutdown(context.Background()))
	}()
	return nil
}
