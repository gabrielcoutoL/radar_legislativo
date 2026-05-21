import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Generator

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from core.exceptions import APIConnectionError, APIRateLimitError

logger = logging.getLogger(__name__)


class ClientAPI:
    def __init__(self):
        self.sessao = requests.Session()
        self.base_url: str = "https://dadosabertos.camara.leg.br/api/v2/"
        self.sessao.headers.update({"Accept": "application/json"})

        self.raw_path = Path("data/raw")
        self.raw_path.mkdir(parents=True, exist_ok=True)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=3, max=30),
        reraise=True,
    )
    def _get_page(self, url: str, params: Dict[str, Any] = None) -> Dict:
        response = self.sessao.get(url=url, params=params, timeout=(5, 30))

        if response.status_code == 429:
            logger.warning("Rate limit atingido")
            raise APIRateLimitError(f"Erro 429 Rate limit: {response.text}")

        if response.status_code in [400, 401, 403, 404]:
            logger.error(f"Erro na requisição: {response.url} - {response.text}")
            raise APIConnectionError(f"Erro {response.status_code}: {response.text}")

        response.raise_for_status()
        return response.json()

    def extrator_api(
        self, endpoint: str, params: Dict[str, Any] = None
    ) -> Generator[Dict, None, None]:
        if params is None:
            params = {}

        params["itens"] = params.get("itens", 100)
        url = f"{self.base_url}{endpoint}"

        logger.info(f"Iniciando paginação do endpoint: {endpoint}")

        while url:
            data = self._get_page(url, params)
            yield data

            params = None
            links = data.get("links", [])
            url = next((link["href"] for link in links if link["rel"] == "next"), None)

    def processar_endpoint(self, endpoint: str, params: Dict[str, Any] = None):

        clean_endpoint = endpoint.strip("/")
        caminho_arquivo = self.raw_path / f"{clean_endpoint}.jsonl"

        if caminho_arquivo.exists():
            caminho_arquivo.unlink()

        pagina = 1
        total_registros = 0

        with open(caminho_arquivo, "a", encoding="utf-8") as f:
            for dados_pagina in self.extrator_api(clean_endpoint, params):
                registros = dados_pagina.get("dados", [])
                total_registros += len(registros)

                for registro in registros:
                    f.write(json.dumps(registro, ensure_ascii=False) + "\n")

                logger.info(
                    f"[{clean_endpoint}] Página {pagina} salva. Acumulado: {total_registros} itens."
                )
                pagina += 1

        return f"Sucesso: {total_registros} registros consolidados em {caminho_arquivo.name}."

    def executar_pipeline_paralelo(self, endpoints: list[str]):
        logger.info("Iniciando pool de threads para extração...")

        with ThreadPoolExecutor(max_workers=3) as executor:
            futuros = {
                executor.submit(self.processar_endpoint, ep): ep for ep in endpoints
            }

            for futuro in as_completed(futuros):
                ep = futuros[futuro]
                try:
                    resultado = futuro.result()
                    logger.info(resultado)
                except Exception as e:
                    logger.error(f"Falha crítica no endpoint {ep}: {e}")
