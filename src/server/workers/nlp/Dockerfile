FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir torch transformers

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

RUN pip install --no-cache-dir -r server/workers/nlp/requirements.txt

# Ejecuta el script
CMD ["python", "server/workers/nlp/main.py"]
