import logging

from src.extract import ClientAPI

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


def main():
    logger = logging.getLogger(__name__)
    logger.info("Iniciando Pipeline Bússola Pública - Extract")

    endpoints_alvo = ["deputados", "proposicoes", "votacoes", "partidos"]

    extrator = ClientAPI()
    extrator.executar_pipeline_paralelo(endpoints_alvo)

    logger.info("Fase de extração concluída.")


if __name__ == "__main__":
    main()
