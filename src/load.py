import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    text,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine

load_dotenv(override=True)

logger = logging.getLogger(__name__)

PROCESSED_PATH = Path("data/processed")

# Número de registros por batch de INSERT.
# 500 para não sobrecarregar o supabase (plano free).
BATCH_SIZE = 500


class Loader:
    """
    Carrega os DataFrames transformados no PostgreSQL (Supabase).
    Responsável pela fase de Load do pipeline ETL.

    Estratégia de carga:
        Upsert via INSERT ... ON CONFLICT DO UPDATE — idempotente por design.
        Rodar o pipeline duas vezes não duplica registros; atualiza os existentes.

    Ordem de carga:
        Dimensões antes dos fatos para respeitar a dependência lógica:
        1. dim_partidos
        2. dim_deputados
        3. fato_proposicoes
        4. fato_votacoes
        5. fato_despesas

    Nota sobre FKs:
        As relações entre tabelas (ex: despesas → deputados) são documentadas
        no schema mas não enforçadas como constraints no banco. Em pipelines
        incrementais, a ordem de chegada dos dados pode violar a constraint
        temporariamente. A integridade é garantida pela ordem de carga acima.

    Configuração (.env):
        DATABASE_URL=postgresql://postgres:[SENHA]@db.[REF].supabase.co:5432/postgres?sslmode=require

        Para usar o connection pooler do Supabase (recomendado em produção):
        DATABASE_URL=postgresql://postgres.[REF]:[SENHA]@aws-0-[REGIAO].pooler.supabase.com:6543/postgres?sslmode=require

    Uso:
        loader = Loader()
        loader.run(tabelas)   # tabelas = saída de Transformer().run()
    """

    def __init__(self):
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise EnvironmentError(
                "DATABASE_URL não encontrada. "
                "Crie um arquivo .env com: DATABASE_URL=postgresql://..."
            )

        self.engine: Engine = create_engine(
            database_url,
            pool_pre_ping=True,  # testa a conexão antes de usar
            pool_size=5,
            max_overflow=2,
        )
        self.metadata = MetaData()
        self._definir_tabelas()

    # ------------------------------------------------------------------
    # Schema das tabelas
    # ------------------------------------------------------------------

    def _definir_tabelas(self) -> None:
        """
        Define o schema completo de todas as tabelas via SQLAlchemy Core.

        Usar Table/Column explícitos (em vez de df.to_sql) dá controle total
        sobre tipos, nullability e PKs — essencial para o ON CONFLICT funcionar.
        """

        # dim_partidos
        self.t_partidos = Table(
            "dim_partidos",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("sigla", String(20), nullable=False),
            Column("nome", String(255)),
            Column("uri", Text),
        )

        # dim_deputados
        self.t_deputados = Table(
            "dim_deputados",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("nome", String(255), nullable=False),
            Column("sigla_partido", String(20)),
            Column("sigla_uf", String(2)),
            Column("id_legislatura", Integer),
            Column("url_foto", Text),
            Column("email", String(255)),
        )

        # fato_proposicoes
        # tema e resumo_executivo são preenchidos na Etapa 4.
        # Criados vazios aqui para que o UPDATE posterior só precise
        # alterar essas duas colunas, sem reprocessar a tabela inteira.
        self.t_proposicoes = Table(
            "fato_proposicoes",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("sigla_tipo", String(20)),
            Column("cod_tipo", Integer),
            Column("numero", Integer),
            Column("ano", Integer),
            Column("ementa", Text),
            Column("data_apresentacao", DateTime(timezone=False)),
            Column("uri", Text),
            Column("tema", String(100)),  # Etapa 4
            Column("resumo_executivo", Text),  # Etapa 4
        )

        # fato_votacoes
        self.t_votacoes = Table(
            "fato_votacoes",
            self.metadata,
            Column("id", String(50), primary_key=True),
            Column("data", Date),
            Column("data_hora_registro", DateTime(timezone=False)),
            Column("sigla_orgao", String(50)),
            Column("descricao", Text),
            Column("aprovacao", Integer),  # 0, 1 ou NULL
            Column("proposicao_objeto", String(255)),
            Column("uri_proposicao_objeto", Text),
            Column("id_proposicao", BigInteger),  # FK lógica → fato_proposicoes
        )

        # fato_despesas
        self.t_despesas = Table(
            "fato_despesas",
            self.metadata,
            Column("id_documento", BigInteger, primary_key=True),
            Column("id_deputado", Integer, nullable=False),
            Column("nome_parlamentar", String(255)),
            Column("data_emissao", DateTime(timezone=False)),
            Column("valor_documento", Float),
            Column("valor_glosa", Float),
            Column("valor_liquido", Float),
            Column("descricao", String(255)),
            Column("descricao_especificacao", String(255)),
            Column("fornecedor", String(255)),
            Column("cnpj_cpf", String(30)),
            Column("numero_documento", String(100)),
            Column("tipo_documento", String(10)),
            Column("mes", Integer),
            Column("ano", Integer),
            Column("url_documento", Text),
        )

    # ------------------------------------------------------------------
    # Infraestrutura do banco
    # ------------------------------------------------------------------

    def _criar_tabelas(self) -> None:
        """
        Cria todas as tabelas no banco caso ainda não existam.

        Usa checkfirst=True implicitamente via create_all — idempotente,
        não recria nem trunca tabelas existentes.
        """
        self.metadata.create_all(self.engine)
        logger.info("Tabelas criadas/verificadas no banco.")

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    def _df_para_registros(self, df: pd.DataFrame) -> list[dict[str, Any]]:
        """
        Converte DataFrame para lista de dicts compatível com o SQLAlchemy.

        Substitui NaN, NaT e pd.NA por None explícito — o PostgreSQL não
        aceita float('nan') como NULL e levantaria DataError no INSERT.
        O astype(object) garante que Int64 nullable seja convertido para
        Python int ou None, não numpy.int64.
        """
        return (
            df.astype(object).where(pd.notna(df), other=None).to_dict(orient="records")
        )

    def _upsert(
        self,
        tabela: Table,
        registros: list[dict[str, Any]],
        pk: str,
    ) -> int:
        """
        Insere ou atualiza registros em lote.

        Usa INSERT ... ON CONFLICT (pk) DO UPDATE SET col = EXCLUDED.col
        para cada coluna não-PK. Se o registro já existir, é atualizado
        com os valores mais recentes; se não existir, é inserido.

        Os registros são enviados em batches de BATCH_SIZE para evitar
        queries com listas muito longas que causam timeout no Supabase.

        Args:
            tabela:    Objeto Table do SQLAlchemy.
            registros: Lista de dicts com os dados a inserir.
            pk:        Nome da coluna de chave primária.

        Returns:
            Total de registros processados.
        """
        if not registros:
            logger.warning(f"[{tabela.name}] Nenhum registro para carregar.")
            return 0

        colunas_update = {
            col: getattr(insert(tabela).excluded, col)
            for col in registros[0].keys()
            if col != pk
        }

        total = 0
        with self.engine.begin() as conn:
            for i in range(0, len(registros), BATCH_SIZE):
                batch = registros[i : i + BATCH_SIZE]
                stmt = insert(tabela).values(batch)

                if colunas_update:
                    stmt = stmt.on_conflict_do_update(
                        index_elements=[pk],
                        set_=colunas_update,
                    )
                else:
                    stmt = stmt.on_conflict_do_nothing(index_elements=[pk])

                conn.execute(stmt)
                total += len(batch)

        logger.info(f"[{tabela.name}] {total} registros upsertados.")
        return total

    def _contar_registros(self, nome_tabela: str) -> int:
        """
        Retorna o total de linhas na tabela após a carga.
        Serve como verificação rápida de sanidade pós-load.
        """
        with self.engine.connect() as conn:
            resultado = conn.execute(text(f"SELECT COUNT(*) FROM {nome_tabela}"))
            total = resultado.scalar()
        logger.info(f"[{nome_tabela}] Total no banco após carga: {total} registros.")
        return total

    # ------------------------------------------------------------------
    # Carga por tabela
    # ------------------------------------------------------------------

    def _load_partidos(self, df: pd.DataFrame) -> None:
        registros = self._df_para_registros(df)
        self._upsert(self.t_partidos, registros, pk="id")
        self._contar_registros("dim_partidos")

    def _load_deputados(self, df: pd.DataFrame) -> None:
        registros = self._df_para_registros(df)
        self._upsert(self.t_deputados, registros, pk="id")
        self._contar_registros("dim_deputados")

    def _load_proposicoes(self, df: pd.DataFrame) -> None:
        registros = self._df_para_registros(df)
        self._upsert(self.t_proposicoes, registros, pk="id")
        self._contar_registros("fato_proposicoes")

    def _load_votacoes(self, df: pd.DataFrame) -> None:
        registros = self._df_para_registros(df)
        self._upsert(self.t_votacoes, registros, pk="id")
        self._contar_registros("fato_votacoes")

    def _load_despesas(self, df: pd.DataFrame) -> None:
        registros = self._df_para_registros(df)
        self._upsert(self.t_despesas, registros, pk="id_documento")
        self._contar_registros("fato_despesas")

    # ------------------------------------------------------------------
    # Execução
    # ------------------------------------------------------------------

    def run(self, tabelas: dict[str, pd.DataFrame]) -> None:
        """
        Executa a carga completa no banco a partir do dicionário de DataFrames.

        Recebe diretamente a saída de Transformer().run():
            {
                "dim_deputados":    DataFrame,
                "dim_partidos":     DataFrame,
                "fato_proposicoes": DataFrame,
                "fato_votacoes":    DataFrame,
                "fato_despesas":    DataFrame,
            }

        Tabelas ausentes no dicionário (ex: despesas ainda não baixadas)
        são ignoradas com aviso — não interrompem a carga das demais.

        Args:
            tabelas: Dicionário retornado por Transformer().run().
        """
        logger.info("=== Iniciando fase de LOAD ===")

        self._criar_tabelas()

        etapas = [
            ("dim_partidos", self._load_partidos),
            ("dim_deputados", self._load_deputados),
            ("fato_proposicoes", self._load_proposicoes),
            ("fato_votacoes", self._load_votacoes),
            ("fato_despesas", self._load_despesas),
        ]

        for chave, fn in etapas:
            if chave not in tabelas:
                logger.warning(f"[{chave}] Ausente no dicionário de tabelas — pulando.")
                continue
            try:
                fn(tabelas[chave])
                logger.info(f"[{chave}] ✓ carga concluída.")
            except Exception as e:
                logger.error(f"[{chave}] Falha na carga: {e}", exc_info=True)

        logger.info("=== LOAD concluído ===")
