FROM golang:1.24.1 AS builder

LABEL intermediateStageToBeDeleted=true

WORKDIR /build/
RUN mkdir protobuf
RUN mkdir client
RUN mkdir vendor
COPY src/go.mod .
COPY src/go.sum .
COPY src/protobuf ./protobuf
COPY src/client ./client
COPY src/vendor ./vendor

RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o bin/client ./client/client.go

FROM busybox:latest
COPY --from=builder /build/bin/client /client
ENTRYPOINT ["/client"]
