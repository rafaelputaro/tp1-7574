FROM golang:1.24.1 AS builder

LABEL intermediateStageToBeDeleted=true

WORKDIR /build/
COPY src/ .

RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o bin/controller ./server/gateway/main.go

FROM busybox:latest
COPY --from=builder /build/bin/controller /controller
ENTRYPOINT ["/controller"]
