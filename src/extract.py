import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Generator, List

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from core.exceptions import APIConnectionError, APIRateLimitError

logger = logging.getLogger(__name__)


class ClientAPI:
    """
    Cliente para a API de Dados Abertos da Câmara dos Deputados.
    Responsável pela fase de Extract do pipeline ETL.

    Funcionalidades:
        - Paginação automática via links HATEOAS retornados pela API
        - Suporte a janelas de data (a API limita intervalos a 90 dias)
        - Retry com backoff em falhas (429, timeout)
        - Extração paralela de múltiplos endpoints via ThreadPoolExecutor
        - Persistência raw em JSONL antes de qualquer transformação

    Arquivos gerados (em data/raw/):
        deputados.jsonl, partidos.jsonl, proposicoes.jsonl, votacoes.jsonl

    Uso:
        extrator = ClientAPI()
        extrator.executar_pipeline_paralelo([
            ("deputados",   [{}]),
            ("proposicoes", [{"dataInicio": "2025-01-01", "dataFim": "2025-03-31"}]),
        ])
    """

    def __init__(self):
        self.sessao = requests.Session()
        self.base_url: str = "https://dadosabertos.camara.leg.br/api/v2/"
        self.sessao.headers.update({"Accept": "application/json"})

        self.raw_path = Path("data/raw")
        self.raw_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Camada HTTP
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=3, max=30),
        reraise=True,
    )
    def _get_page(self, url: str, params: Dict[str, Any] = None) -> Dict:
        """
        Executa uma requisição GET com retry automático.

        A política de retry (tenacity) tenta até 3 vezes com espera
        exponencial de 3s, 6s e 12s entre tentativas. Erros 4xx são
        relançados imediatamente sem retry — são falhas de configuração,
        não falhas transitórias de rede.

        Args:
            url:    URL completa ou parcial do endpoint.
            params: Query parameters da requisição (paginação, filtros de data etc.).

        Returns:
            Dicionário com a resposta JSON da API.

        Raises:
            APIRateLimitError:  HTTP 429 — limite de requisições atingido.
            APIConnectionError: HTTP 400/401/403/404 — erro na requisição.
            requests.HTTPError: Qualquer outro status >= 400.
        """
        response = self.sessao.get(url=url, params=params, timeout=(5, 30))

        if response.status_code == 429:
            logger.warning("Rate limit atingido")
            raise APIRateLimitError(f"Erro 429 Rate limit: {response.text}")

        if response.status_code in [400, 401, 403, 404]:
            logger.error(f"Erro na requisição: {response.url} - {response.text}")
            raise APIConnectionError(f"Erro {response.status_code}: {response.text}")

        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Paginação
    # ------------------------------------------------------------------

    def extrator_api(
        self, endpoint: str, params: Dict[str, Any] = None
    ) -> Generator[Dict, None, None]:
        """
        Itera sobre todas as páginas de um endpoint via links HATEOAS.

        A API da Câmara retorna um campo 'links' em cada resposta com
        rel='next' apontando para a próxima página. Quando esse link
        não existe, a paginação termina.

        Os params são enviados apenas na primeira requisição; nas
        seguintes a URL já os carrega embutidos no link 'next'.

        Args:
            endpoint: Caminho relativo do endpoint (ex: "proposicoes").
            params:   Query parameters iniciais. 'itens' é definido como
                      100 se não informado (máximo recomendado pela API).

        Yields:
            Dicionário com a resposta bruta de cada página, incluindo
            as chaves 'dados' e 'links'.
        """
        if params is None:
            params = {}

        params["itens"] = params.get("itens", 100)
        url = f"{self.base_url}{endpoint}"

        logger.info(f"Iniciando paginação do endpoint: {endpoint}")

        while url:
            data = self._get_page(url, params)
            yield data

            # Params são descartados após a primeira página — as próximas
            # URLs já os carregam embutidos no link HATEOAS
            params = None
            links = data.get("links", [])
            url = next((link["href"] for link in links if link["rel"] == "next"), None)

    # ------------------------------------------------------------------
    # Persistência
    # ------------------------------------------------------------------

    def processar_endpoint(
        self, endpoint: str, params_list: List[Dict[str, Any]] = None
    ) -> str:
        """
        Extrai um endpoint completo (todas as janelas de data) e persiste
        em JSONL.

        Itera sobre cada entrada de params_list como uma janela temporal
        independente. Todos os registros são acumulados no mesmo arquivo,
        que é recriado do zero a cada execução para garantir idempotência.

        Por que JSONL?
            Permite append linha a linha sem carregar o arquivo em memória.
            Se o Transform quebrar, a extração não precisa ser refeita.

        Args:
            endpoint:    Caminho relativo do endpoint (ex: "proposicoes").
            params_list: Lista de dicionários de parâmetros, um por janela
                         de data. Ex:
                         [
                             {"dataInicio": "2025-01-01", "dataFim": "2025-03-31"},
                             {"dataInicio": "2025-04-01", "dataFim": "2025-06-29"},
                         ]
                         Passar [{}] para endpoints sem filtro de data
                         (ex: deputados, partidos).

        Returns:
            Mensagem de sucesso com total de registros e nome do arquivo.
        """
        if params_list is None:
            params_list = [{}]

        clean_endpoint = endpoint.strip("/")
        caminho_arquivo = self.raw_path / f"{clean_endpoint}.jsonl"

        # Recria o arquivo a cada execução — garante idempotência
        if caminho_arquivo.exists():
            caminho_arquivo.unlink()

        total_registros = 0
        total_janelas = len(params_list)

        with open(caminho_arquivo, "a", encoding="utf-8") as f:
            for idx, params in enumerate(params_list, start=1):
                pagina = 1
                janela_label = (
                    f"{params.get('dataInicio', 'sem-filtro')} → {params.get('dataFim', '')}"
                    if params
                    else "sem filtro de data"
                )
                logger.info(
                    f"[{clean_endpoint}] Janela {idx}/{total_janelas}: {janela_label}"
                )

                for dados_pagina in self.extrator_api(clean_endpoint, params):
                    registros = dados_pagina.get("dados", [])
                    total_registros += len(registros)

                    for registro in registros:
                        f.write(json.dumps(registro, ensure_ascii=False) + "\n")

                    logger.info(
                        f"[{clean_endpoint}] Janela {idx}/{total_janelas} | "
                        f"Página {pagina} | Acumulado: {total_registros} itens."
                    )
                    pagina += 1

        return f"Sucesso: {total_registros} registros consolidados em {caminho_arquivo.name}."

    # ------------------------------------------------------------------
    # Orquestração paralela
    # ------------------------------------------------------------------

    def executar_pipeline_paralelo(
        self, endpoints: list[tuple[str, List[Dict[str, Any]]]]
    ) -> None:
        """
        Extrai múltiplos endpoints em paralelo usando um pool de threads.

        Cada endpoint roda em sua própria thread. Falhas em um endpoint
        são logadas sem interromper os demais. O limite de 3 workers
        evita sobrecarga na API pública da Câmara.

        Args:
            endpoints: Lista de tuplas (endpoint, params_list). Ex:
                       [
                           ("deputados",   [{}]),
                           ("partidos",    [{}]),
                           ("proposicoes", [{"dataInicio": "...", "dataFim": "..."}]),
                           ("votacoes",    [{"dataInicio": "...", "dataFim": "..."}]),
                       ]
        """
        logger.info("Iniciando pool de threads para extração...")

        with ThreadPoolExecutor(max_workers=3) as executor:
            futuros = {
                executor.submit(self.processar_endpoint, ep, params_list): ep
                for ep, params_list in endpoints
            }

            for futuro in as_completed(futuros):
                ep = futuros[futuro]
                try:
                    resultado = futuro.result()
                    logger.info(resultado)
                except Exception as e:
                    logger.error(f"Falha crítica no endpoint {ep}: {e}")
