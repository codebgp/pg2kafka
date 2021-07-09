#!/bin/bash

go get github.com/sqs/goreturns && \
	go get -u github.com/securego/gosec/v2/cmd/gosec@v2.7.0 && \
	go get -u golang.org/x/lint/golint && \
	go get github.com/golangci/golangci-lint/cmd/golangci-lint@v1.40.0
