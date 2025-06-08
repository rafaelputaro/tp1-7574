# TODO: Try smaller images?
FROM golang:1.24.1 AS builder

LABEL intermediateStageToBeDeleted=true

WORKDIR /build/

RUN mkdir vendor
RUN mkdir health
RUN mkdir server
COPY src/go.mod .
COPY src/go.sum .
COPY src/vendor ./vendor
COPY src/health ./health
COPY src/server ./server

# RUN apk add --no-cache docker openrc

# RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o resilience_manager ./server/resilience_manager
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o bin/resilience_manager ./server/resilience_manager/main.go

FROM busybox:latest
COPY --from=builder /build/bin/resilience_manager /resilience_manager
ENTRYPOINT ["/resilience_manager"]
