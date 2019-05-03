#!/bin/bash
go build -o connector_lib.so -buildmode=c-shared sharedlib.go