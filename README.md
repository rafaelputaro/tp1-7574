# Trabajo Práctico Sistemas Distribuidos

## Como levantar el sistema:

1. Configurar parámetros del sistema en el archivo `config.ini`.
2. Ejecutar `python3 compose-generator.py` para crear el compose file.
3. Ejecutar `make docker-compose-up`.
4. Ejecutar `make docker-compose-logs` para ver los logs de los nodos.
5. Ejecutar `make docker-compose-down` para bajar el sistema.

## Client

```bash
go build -o client client.go

sudo chmod +x generate_report.sh

./generate_report.sh
```

## Dependencies & Compilation

Poner los datasets en la carpeta `src/client/datasets/`.

Ejecutar los siguientes comandos desde la carpeta `tp1-7574/`

Instalar dependencias

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

go mod download
go get -u all
go mod tidy
go mod vendor
```

Generar archivos proto

```bash
protoc -I=./src/protobuf --go_out=./src/protobuf --go-grpc_out=./src/protobuf ./src/protobuf/*.proto
```

## Tests Automáticos

Ejecutar en la carpeta `src/`

```bash
go test ./...
```

## Tests Manuales

```bash
docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

Go to http://localhost:15672/

## Fuentes:

* RabbitMQ + Golang tutorial: https://www.rabbitmq.com/tutorials/tutorial-one-go
