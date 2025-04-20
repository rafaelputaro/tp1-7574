FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir torch transformers

COPY src/ .

RUN pip install --no-cache-dir -r server/workers/nlp/requirements.txt

# Ejecuta el script
CMD ["python", "server/workers/nlp/main.py"]
