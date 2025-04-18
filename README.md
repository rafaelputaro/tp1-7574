# Trabajo Práctico Sistemas Distribuidos

## Como levantar el sistema:
1. Configurar parámetros del sistema en el archivo `config.ini`.
2. Ejecutar `python3 compose-generator.py` para crear el compose file.
3. Ejecutar `make docker-compose-up`.
4. Ejecutar `make docker-compose-logs` para ver los logs de los nodos.


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
go mod download
```

Generar archivos proto

```bash
protoc -I=./src/protobuf --go_out=./src/protobuf --go-grpc_out=./src/protobuf ./src/protobuf/*.proto
```

## Tests

Ejecutar en la carpeta `src/`

```bash
go test ./...
```
