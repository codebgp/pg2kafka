FROM golang:1.15-alpine AS builder

# needed for gcc
RUN apk add --no-cache build-base && \
    apk add --no-cache git && \
    apk add --no-cache --upgrade bash

WORKDIR /build
COPY . .

RUN go build -trimpath -tags musl -ldflags "-X main.Version=$(git rev-parse --short @) -s -extldflags -static" -a -installsuffix cgo .

FROM scratch

LABEL maintainer="dev[at]codebgp[dot]com"

COPY --from=builder /build/pg2kafka /
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
ENTRYPOINT ["/pg2kafka"]
