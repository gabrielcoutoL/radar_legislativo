import logging
import os
from typing import Any

import numpy as np
from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv(override=True)

logger = logging.getLogger(__name__)

BATCH_SIZE = 2000


class Enricher:
    """
    Enriquece fato_proposicoes com classificação temática via embeddings.

    Estratégia (Caminho A):
        1. Gera embeddings para ~10 temas pré-definidos (uma única vez).
        2. Lê proposições sem tema do banco em batches de BATCH_SIZE.
        3. Gera embeddings das ementas via text-embedding-3-small.
        4. Calcula similaridade de cosseno entre cada ementa e cada tema.
        5. Atribui o tema de maior similaridade e persiste no banco.

    Modelo: text-embedding-3-small (OpenAI)
        Custo: $0.020 / 1M tokens
        Estimativa para 161k proposições (~100 tokens/ementa): ~$0.32 USD

    Idempotência:
        A query usa WHERE tema IS NULL — rodar duas vezes não reprocessa
        proposições já classificadas. Pode ser interrompido e retomado.

    Configuração necessária (.env):
        OPENAI_API_KEY=sk-proj-...
        DATABASE_URL=postgresql://...
    """

    # Temas com descrições densas de palavras-chave.
    # Quanto mais rica a descrição, melhor a discriminação do embedding.
    TEMAS: dict[str, str] = {
        "Saúde": (
            "saúde pública SUS medicamentos hospitais doenças vacinas "
            "planos de saúde vigilância sanitária enfermagem médicos "
            "farmácias unidades de saúde tratamento clínico"
        ),
        "Tributário": (
            "impostos tributos arrecadação isenção fiscal reforma tributária "
            "ICMS IPI imposto de renda IR alíquota receita federal "
            "contribuição fiscal sonegação crédito tributário"
        ),
        "Trabalho": (
            "emprego trabalhadores CLT salário mínimo sindicatos previdência "
            "aposentadoria FGTS demissão contrato de trabalho licença "
            "maternidade paternidade jornada horas extras terceirização"
        ),
        "Tecnologia": (
            "tecnologia inteligência artificial IA internet dados pessoais "
            "LGPD digital software inovação cibersegurança telecomunicações "
            "5G algoritmos automação plataformas digitais"
        ),
        "Meio Ambiente": (
            "meio ambiente sustentabilidade desmatamento mudança climática "
            "poluição biodiversidade recursos hídricos fauna flora resíduos "
            "licenciamento ambiental agrotóxicos carbono energia renovável"
        ),
        "Educação": (
            "educação escolas universidades ensino professores bolsas de estudo "
            "MEC ensino médio fundamental alfabetização EAD pesquisa científica "
            "FIES ProUni cotas currículo escolar merenda"
        ),
        "Segurança Pública": (
            "segurança pública polícia crimes violência prisões tráfico de drogas "
            "armamento Código Penal investigação penitenciária milícia homicídio "
            "combate ao crime organizado sistema prisional"
        ),
        "Economia": (
            "economia PIB inflação juros banco central crédito mercado financeiro "
            "câmbio exportação importação comércio competitividade indústria "
            "microempreendedor MEI privatização concessão"
        ),
        "Direitos Civis": (
            "direitos humanos igualdade discriminação minorias gênero raça "
            "acessibilidade pessoa com deficiência LGBTQIA mulheres criança "
            "adolescente idosos violência doméstica estatuto"
        ),
        "Infraestrutura": (
            "obras públicas rodovias saneamento básico energia elétrica habitação "
            "transporte público portos aeroportos ferrovias urbanização "
            "mobilidade urbana concessão infraestrutura logística"
        ),
    }

    def __init__(self):
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise EnvironmentError(
                "OPENAI_API_KEY não encontrada. "
                "Adicione ao .env: OPENAI_API_KEY=sk-proj-..."
            )

        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise EnvironmentError("DATABASE_URL não encontrada no .env.")

        # LangChain abstrai a chamada à API da OpenAI.
        # chunk_size controla o tamanho máximo de cada requisição interna.
        self.embeddings = OpenAIEmbeddings(
            model="text-embedding-3-small",
            api_key=api_key,
            chunk_size=200,
            timeout=30,  # desiste após 30s sem resposta — evita o freeze
            max_retries=3,  # tenta 3 vezes antes de levantar exceção
        )

        self.engine: Engine = create_engine(
            database_url,
            pool_pre_ping=True,
            pool_size=3,
        )

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    def _gerar_embeddings_temas(self) -> dict[str, np.ndarray]:
        """
        Gera embeddings para todos os temas de uma vez.
        Retorna dicionário {nome_tema: vetor_numpy}.

        Esta operação custa apenas 10 tokens (um por tema) e é feita
        uma única vez antes do loop principal.
        """
        logger.info(f"Gerando embeddings para {len(self.TEMAS)} temas...")
        nomes = list(self.TEMAS.keys())
        descricoes = list(self.TEMAS.values())

        vetores = self.embeddings.embed_documents(descricoes)

        # Converter para numpy para operações vetorizadas na similaridade
        resultado = {nome: np.array(vetor) for nome, vetor in zip(nomes, vetores)}
        logger.info(f"Embeddings de temas prontos: {nomes}")
        return resultado

    def _classificar_batch(
        self,
        ementas: list[str],
        embeddings_temas: dict[str, np.ndarray],
    ) -> list[str]:
        """
        Classifica um batch de ementas por similaridade de cosseno.

        Os embeddings do text-embedding-3-small já são normalizados
        (norma L2 = 1), portanto o produto escalar é equivalente à
        similaridade de cosseno — sem necessidade de normalização extra.

        Args:
            ementas:          Lista de textos de ementa a classificar.
            embeddings_temas: Dicionário {tema: vetor} gerado uma vez pelo
                              método _gerar_embeddings_temas.

        Returns:
            Lista de strings com o tema mais similar para cada ementa,
            na mesma ordem da lista de entrada.
        """
        # Gerar embeddings das ementas — LangChain lida com batching interno
        MAX_CHARS = 500
        ementas_truncadas = [e[:MAX_CHARS] if e else "" for e in ementas]

        vetores_ementas = self.embeddings.embed_documents(ementas_truncadas)

        # Matriz de temas: shape (n_temas, n_dimensoes) = (10, 1536)
        nomes_temas = list(embeddings_temas.keys())
        matriz_temas = np.array(list(embeddings_temas.values()))

        temas_classificados = []
        for vetor in vetores_ementas:
            v = np.array(vetor)  # (1536,)

            # Produto escalar com cada linha da matriz = similaridade de cosseno
            # Resultado: vetor (10,) com um score por tema
            similaridades = matriz_temas @ v  # broadcasting matricial

            idx_max = int(np.argmax(similaridades))
            temas_classificados.append(nomes_temas[idx_max])

        return temas_classificados

    def _contar_pendentes(self) -> int:
        """Retorna quantas proposições ainda não foram classificadas."""
        with self.engine.connect() as conn:
            resultado = conn.execute(
                text("SELECT COUNT(*) FROM fato_proposicoes WHERE tema IS NULL")
            )
            return resultado.scalar()

    def _atualizar_temas(self, updates: list[dict[str, Any]]) -> None:
        """
        Persiste os temas via uma única query UPDATE com VALUES.

        Substitui o executemany (N queries individuais) por uma query só:
            UPDATE fato_proposicoes AS fp
            SET tema = v.tema
            FROM (VALUES (id1,'Tema1'), (id2,'Tema2'), ...) AS v(id, tema)
            WHERE fp.id = v.id::integer

        Com 2000 registros: 1 round-trip de rede em vez de 2000.
        Os temas são strings controladas da lista TEMAS — sem risco de injection.
        """
        if not updates:
            return

        rows = ", ".join(f"({u['id']}, '{u['tema']}')" for u in updates)

        with self.engine.begin() as conn:
            conn.execute(
                text(f"""
                UPDATE fato_proposicoes AS fp
                SET tema = v.tema
                FROM (VALUES {rows}) AS v(id, tema)
                WHERE fp.id = v.id::integer
            """)
            )

    def _estimar_custo(self, n_proposicoes: int) -> str:
        """Retorna estimativa de custo formatada para log."""
        # text-embedding-3-small: $0.020 / 1M tokens
        # Média estimada de 100 tokens por ementa
        tokens_estimados = n_proposicoes * 100
        custo_usd = (tokens_estimados / 1_000_000) * 0.020
        custo_brl = custo_usd * 5.70  # câmbio aproximado
        return (
            f"~{tokens_estimados:,} tokens | ~${custo_usd:.2f} USD | ~R${custo_brl:.2f}"
        )

    # ------------------------------------------------------------------
    # Ponto de entrada público
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Executa a classificação temática para todas as proposições pendentes.

        Fluxo:
            1. Conta proposições com tema IS NULL.
            2. Gera embeddings dos 10 temas (chamada única à API).
            3. Loop: lê BATCH_SIZE proposições → gera embeddings → classifica
            → persiste → repete até não haver mais pendentes.

        Resiliência:
            - timeout=30s no cliente OpenAI evita que requests travem indefinidamente.
            - Batch com falha recebe tema "Outros" e o pipeline avança — sem travar.
            - WHERE tema IS NULL garante retomada sem reprocessar o que já foi feito.
        """
        logger.info("=== Iniciando fase de IA — Classificação temática ===")

        pendentes = self._contar_pendentes()
        if pendentes == 0:
            logger.info("Todas as proposições já estão classificadas. Nada a fazer.")
            return

        logger.info(f"Proposições a classificar: {pendentes:,}")
        logger.info(f"Estimativa de custo: {self._estimar_custo(pendentes)}")
        logger.info(
            "Pressione Ctrl+C para interromper — o progresso é salvo a cada batch."
        )

        # Passo 1: embeddings dos temas — custo fixo, feito uma única vez
        embeddings_temas = self._gerar_embeddings_temas()

        # Passo 2: loop de classificação em batches
        total_classificadas = 0

        while True:
            # Sempre lê as primeiras BATCH_SIZE com tema IS NULL.
            # Após o UPDATE do batch anterior, elas somem da query naturalmente.
            with self.engine.connect() as conn:
                resultado = conn.execute(
                    text(
                        "SELECT id, ementa FROM fato_proposicoes "
                        "WHERE tema IS NULL "
                        "ORDER BY id "
                        "LIMIT :limit"
                    ),
                    {"limit": BATCH_SIZE},
                )
                rows = resultado.fetchall()

            if not rows:
                break

            ids = [r[0] for r in rows]
            ementas = [r[1] or "" for r in rows]

            n_batch = total_classificadas // BATCH_SIZE + 1
            logger.info(
                f"Batch {n_batch} | {len(ementas)} proposições | "
                f"classificadas até agora: {total_classificadas:,}/{pendentes:,}"
            )

            try:
                # Caminho normal: classifica e persiste os temas reais
                temas = self._classificar_batch(ementas, embeddings_temas)
                updates = [{"id": id_, "tema": tema} for id_, tema in zip(ids, temas)]
                self._atualizar_temas(updates)
                logger.info(
                    f"Batch salvo. Acumulado: {total_classificadas + len(rows):,}/{pendentes:,}"
                )

            except Exception as e:
                # Caminho de fallback: timeout, 431, rate limit, etc.
                # Marca o batch com "Outros" para não ficar preso nas mesmas
                # proposições problemáticas a cada retomada do pipeline.
                logger.error(f"Batch {n_batch} falhou ({e.__class__.__name__}: {e})")
                logger.warning(
                    f"Marcando {len(ids)} proposições do batch {n_batch} "
                    f"como 'Outros' para avançar. Reclassifique depois se necessário."
                )
                updates_fallback = [{"id": id_, "tema": "Outros"} for id_ in ids]
                self._atualizar_temas(updates_fallback)

            total_classificadas += len(rows)

        logger.info(
            f"=== IA concluída: {total_classificadas:,} proposições classificadas ==="
        )
