import logging
from transformers import pipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sentiment")

def main():
    logger.info("Cargando modelo de análisis de sentimientos...")
    sentiment_analyzer = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')

    texto = "I love this!"
    logger.info(f"Analizando: {texto}")

    resultado = sentiment_analyzer(texto)
    logger.info(f"Resultado: {resultado}")

if __name__ == "__main__":
    main()
