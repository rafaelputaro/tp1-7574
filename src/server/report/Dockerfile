FROM golang:1.24.1 AS builder

LABEL intermediateStageToBeDeleted=true

WORKDIR /build/
COPY src/ .

RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -o bin/report ./server/report/report.go

FROM busybox:latest
COPY --from=builder /build/bin/report /report
ENTRYPOINT ["/report"]
