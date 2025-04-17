# Trabajo Práctico Sistemas Distribuidos

## Client

Poner los datasets en la carpeta `src/client/datasets/`.

```bash
go build -o client client.go

sudo chmod +x generate_report.sh

./generate_report.sh
```

## Generar archivos proto:

Instalar dependencias

```bash
go mod download
```

Generar archivos proto (ejecutar desde la carpeta `tp1-7574/`):

```bash
protoc -I=./src/protobuf --go_out=./src/protobuf --go-grpc_out=./src/protobuf ./src/protobuf/*.proto
```
