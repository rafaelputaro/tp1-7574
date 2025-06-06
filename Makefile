SHELL := /bin/bash

default: docker-image

deps:
	protoc -I=./src/protobuf --go_out=./src/protobuf --go-grpc_out=./src/protobuf ./src/protobuf/*.proto
	protoc -I=./src/protobuf --python_out=./src/server/workers/nlp ./src/protobuf/movie_sanit.proto ./src/protobuf/coordination.proto
	cd src && go mod tidy
	cd src && go mod vendor
.PHONY: deps

docker-compose-dev.yaml: compose-generator.py config.ini
	python3 compose-generator.py
.PHONY: docker-compose-dev.yaml

docker-image: deps
	docker build -f ./src/server/workers/filter/Dockerfile -t "filter:latest" .
	docker build -f ./src/server/workers/nlp/Dockerfile -t "nlp:latest" .
	docker build -f ./src/server/workers/joiner/Dockerfile -t "joiner:latest" .
	docker build -f ./src/server/workers/aggregator/Dockerfile -t "aggregator:latest" .
	docker build -f ./src/server/gateway/Dockerfile -t "controller:latest" .
	docker build -f ./src/server/report/Dockerfile -t "report:latest" .
	docker build -f ./src/client/Dockerfile -t "client:latest" .
.PHONY: docker-image

docker-compose-up: docker-compose-dev.yaml docker-image
	docker compose -f docker-compose-dev.yaml down --remove-orphans || true
	docker compose -f docker-compose-dev.yaml up -d --build --force-recreate
.PHONY: docker-compose-up

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs

docker-compose-down:
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down
