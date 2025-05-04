FROM golang:1.24.1 AS builder

LABEL intermediateStageToBeDeleted=true

WORKDIR /build/
RUN mkdir protobuf
RUN mkdir rabbitmq
RUN mkdir globalconfig
RUN mkdir server
RUN mkdir vendor
COPY src/go.mod .
COPY src/go.sum .
COPY src/protobuf ./protobuf
COPY src/rabbitmq ./rabbitmq
COPY src/globalconfig ./globalconfig
COPY src/server ./server
COPY src/vendor ./vendor

RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o bin/controller ./server/gateway/main.go

FROM busybox:latest
COPY --from=builder /build/bin/controller /controller
ENTRYPOINT ["/controller"]
