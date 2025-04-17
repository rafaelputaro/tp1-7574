# Trabajo Práctico Sistemas Distribuidos

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
