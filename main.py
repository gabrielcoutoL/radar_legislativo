import argparse
import logging
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from src.extract import ClientAPI
from src.load import Loader
from src.transform import Transformer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

PROCESSED_PATH = Path("data/processed")


# ------------------------------------------------------------------
# Utilitário de datas
# ------------------------------------------------------------------


def gerar_janelas(inicio: date, fim: date, max_dias: int = 90) -> list[tuple[str, str]]:
    """
    Divide um intervalo em janelas de até max_dias dias,
    respeitando o limite de 3 meses da API da Câmara.
    Retorna lista de tuplas (data_inicio_iso, data_fim_iso).
    """
    janelas = []
    atual = inicio
    while atual <= fim:
        fim_janela = min(atual + timedelta(days=max_dias - 1), fim)
        janelas.append((atual.isoformat(), fim_janela.isoformat()))
        atual = fim_janela + timedelta(days=1)
    return janelas


# ------------------------------------------------------------------
# Fases do pipeline
# ------------------------------------------------------------------


def fase_extract(logger: logging.Logger) -> None:
    """
    Fase 1 — Extract: chama a API da Câmara e persiste os JSONLs em data/raw/.
    Endpoints com filtro de data são divididos em janelas de 90 dias.
    """
    logger.info("=== Fase 1: EXTRACT ===")

    inicio = date(2025, 1, 1)
    fim = date.today()
    janelas = gerar_janelas(inicio, fim)

    logger.info(
        f"Período total: {inicio.isoformat()} → {fim.isoformat()} "
        f"({len(janelas)} janela(s) de 90 dias)"
    )

    params_com_data = [{"dataInicio": ini, "dataFim": fim_j} for ini, fim_j in janelas]

    endpoints_alvo = [
        ("deputados", [{}]),  # sem filtro de data
        ("partidos", [{}]),  # sem filtro de data
        ("proposicoes", params_com_data),  # janelas de 90 dias
        ("votacoes", params_com_data),  # janelas de 90 dias
    ]

    ClientAPI().executar_pipeline_paralelo(endpoints_alvo)


def fase_transform(logger: logging.Logger) -> dict[str, pd.DataFrame]:
    """
    Fase 2 — Transform: lê os JSONLs de data/raw/, aplica validações
    e transformações, salva parquets em data/processed/ e retorna
    o dicionário de DataFrames para a fase de Load.
    """
    logger.info("=== Fase 2: TRANSFORM ===")
    return Transformer().run()


def fase_load(
    logger: logging.Logger,
    tabelas: dict[str, pd.DataFrame] | None = None,
) -> None:
    """
    Fase 3 — Load: carrega os DataFrames no PostgreSQL (Supabase).

    Se tabelas for None (execução isolada com --fase load), lê os
    parquets de data/processed/ produzidos pela última execução do Transform.
    Isso permite reprocessar a carga sem repetir a extração.
    """
    logger.info("=== Fase 3: LOAD ===")

    if tabelas is None:
        logger.info(
            "Nenhum DataFrame recebido — carregando parquets de data/processed/."
        )
        tabelas = _ler_parquets(logger)

    Loader().run(tabelas)


def _ler_parquets(logger: logging.Logger) -> dict[str, pd.DataFrame]:
    """
    Lê os parquets gerados pelo Transform e devolve o mesmo formato
    de dicionário esperado pelo Loader.
    Usado quando Load é executado de forma isolada (--fase load).
    """
    nomes = [
        "dim_deputados",
        "dim_partidos",
        "fato_proposicoes",
        "fato_votacoes",
        "fato_despesas",
    ]
    tabelas: dict[str, pd.DataFrame] = {}
    for nome in nomes:
        path = PROCESSED_PATH / f"{nome}.parquet"
        if path.exists():
            tabelas[nome] = pd.read_parquet(path)
            logger.info(
                f"[{nome}] {len(tabelas[nome])} registros carregados do parquet."
            )
        else:
            logger.warning(
                f"[{nome}] {path} não encontrado — tabela será ignorada no Load."
            )
    return tabelas


# ------------------------------------------------------------------
# Orquestrador principal
# ------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline ETL Bússola Pública — API da Câmara dos Deputados.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--fase",
        choices=["tudo", "extract", "transform", "load"],
        default="tudo",
        help=(
            "Fase a executar (padrão: tudo):\n"
            "  tudo       — executa extract → transform → load em sequência\n"
            "  extract    — apenas coleta da API e persiste JSONLs\n"
            "  transform  — apenas lê JSONLs, valida e salva parquets\n"
            "  load       — apenas carrega parquets no banco (requer transform anterior)\n"
        ),
    )
    args = parser.parse_args()

    logger = logging.getLogger(__name__)
    inicio_total = time.perf_counter()
    logger.info(f"Pipeline Bússola Pública iniciado — fase: '{args.fase}'")

    if args.fase == "tudo":
        _executar_fase("Extract", fase_extract, logger)
        tabelas = _executar_fase("Transform", fase_transform, logger)
        _executar_fase("Load", fase_load, logger, tabelas=tabelas)

    elif args.fase == "extract":
        _executar_fase("Extract", fase_extract, logger)

    elif args.fase == "transform":
        _executar_fase("Transform", fase_transform, logger)

    elif args.fase == "load":
        _executar_fase("Load", fase_load, logger)

    duracao = time.perf_counter() - inicio_total
    logger.info(f"Pipeline concluído em {duracao:.1f}s.")


def _executar_fase(nome: str, fn, logger: logging.Logger, **kwargs):
    """
    Executa uma fase do pipeline medindo o tempo e capturando erros.
    Erros são logados mas não interrompem as fases seguintes quando
    rodando em modo 'tudo' — permite inspecionar o banco parcialmente
    populado mesmo em caso de falha em uma etapa específica.

    Retorna o resultado da função ou None em caso de erro.
    """
    t0 = time.perf_counter()
    try:
        resultado = fn(logger, **kwargs)
        logger.info(f"[{nome}] concluído em {time.perf_counter() - t0:.1f}s.")
        return resultado
    except Exception as e:
        logger.error(
            f"[{nome}] falhou após {time.perf_counter() - t0:.1f}s: {e}", exc_info=True
        )
        return None


if __name__ == "__main__":
    main()
