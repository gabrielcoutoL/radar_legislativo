import json
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

RAW_PATH = Path("data/raw")
PROCESSED_PATH = Path("data/processed")


class Transformer:
    """
    Lê os arquivos brutos de data/raw/, aplica validações e transformações,
    salva parquets em data/processed/ e devolve um dicionário de DataFrames
    prontos para carga no banco.

    Tabelas geradas:
        Dimensões : dim_deputados, dim_partidos
        Fatos     : fato_proposicoes, fato_votacoes, fato_despesas
    """

    def __init__(
        self,
        raw_path: Path = RAW_PATH,
        processed_path: Path = PROCESSED_PATH,
    ):
        self.raw_path = raw_path
        self.processed_path = processed_path
        self.processed_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _ler_jsonl(self, nome: str) -> pd.DataFrame:
        """Lê um arquivo JSONL"""
        path = self.raw_path / f"{nome}.jsonl"
        if not path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {path}")
        df = pd.read_json(path, lines=True)
        logger.info(f"[{nome}] {len(df)} registros carregados.")
        return df

    def _ler_json_dados(self, nome: str) -> pd.DataFrame:
        """
        Lê JSONs dos arquivos de
        despesas baixados do portal da Câmara.
        """
        path = self.raw_path / f"{nome}.json"
        if not path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {path}")
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
        registros = raw.get("dados", raw) if isinstance(raw, dict) else raw
        df = pd.json_normalize(registros)
        logger.info(f"[{nome}] {len(df)} registros carregados.")
        return df

    def _validar_nulos(
        self,
        df: pd.DataFrame,
        campos_obrigatorios: list[str],
        entidade: str,
    ) -> pd.DataFrame:
        """Remove registros com nulos em campos obrigatórios e loga o aviso."""
        mask = df[campos_obrigatorios].isnull().any(axis=1)
        n = mask.sum()
        if n:
            logger.warning(
                f"[{entidade}] {n} registros com nulos em {campos_obrigatorios} → removidos."
            )
            df = df[~mask].copy()
        return df

    def _deduplicar(self, df: pd.DataFrame, pk: str, entidade: str) -> pd.DataFrame:
        """Remove duplicatas pela chave primária e loga quantas foram removidas."""
        antes = len(df)
        df = df.drop_duplicates(subset=[pk]).copy()
        removidos = antes - len(df)
        if removidos:
            logger.warning(
                f"[{entidade}] {removidos} duplicata(s) removida(s) (pk={pk})."
            )
        return df

    def _salvar_parquet(self, df: pd.DataFrame, nome: str) -> None:
        path = self.processed_path / f"{nome}.parquet"
        df.to_parquet(path, index=False)
        logger.info(f"[{nome}] Salvo em {path}.")

    # ------------------------------------------------------------------
    # dim_deputados
    # ------------------------------------------------------------------

    def _transform_deputados(self) -> pd.DataFrame:
        df = self._ler_jsonl("deputados")

        # Selecionar e renomear para snake_case
        df = df.rename(
            columns={
                "siglaPartido": "sigla_partido",
                "siglaUf": "sigla_uf",
                "idLegislatura": "id_legislatura",
                "urlFoto": "url_foto",
            }
        )[
            [
                "id",
                "nome",
                "sigla_partido",
                "sigla_uf",
                "id_legislatura",
                "url_foto",
                "email",
            ]
        ]

        df = self._validar_nulos(df, ["id", "nome"], "deputados")
        df = self._deduplicar(df, "id", "deputados")

        # Tipos
        df["id"] = df["id"].astype(int)
        df["id_legislatura"] = df["id_legislatura"].astype(int)

        # Limpeza de strings
        df["nome"] = df["nome"].str.strip()
        df["sigla_partido"] = df["sigla_partido"].str.strip().str.upper()
        df["sigla_uf"] = df["sigla_uf"].str.strip().str.upper()
        df["email"] = df["email"].str.strip().str.lower()

        logger.info(f"[deputados] {len(df)} registros prontos.")
        return df

    # ------------------------------------------------------------------
    # dim_partidos
    # ------------------------------------------------------------------

    def _transform_partidos(self) -> pd.DataFrame:
        df = self._ler_jsonl("partidos")

        df = df[["id", "sigla", "nome", "uri"]]

        df = self._validar_nulos(df, ["id", "sigla"], "partidos")
        df = self._deduplicar(df, "id", "partidos")

        df["id"] = df["id"].astype(int)
        df["sigla"] = df["sigla"].str.strip().str.upper()
        df["nome"] = df["nome"].str.strip()

        logger.info(f"[partidos] {len(df)} registros prontos.")
        return df

    # ------------------------------------------------------------------
    # fato_proposicoes
    # ------------------------------------------------------------------

    def _transform_proposicoes(self) -> pd.DataFrame:
        df = self._ler_jsonl("proposicoes")

        df = df.rename(
            columns={
                "siglaTipo": "sigla_tipo",
                "codTipo": "cod_tipo",
                "dataApresentacao": "data_apresentacao",
            }
        )[
            [
                "id",
                "sigla_tipo",
                "cod_tipo",
                "numero",
                "ano",
                "ementa",
                "data_apresentacao",
                "uri",
            ]
        ]

        df = self._validar_nulos(df, ["id", "ementa"], "proposicoes")
        df = self._deduplicar(df, "id", "proposicoes")

        # Tipos
        df["id"] = df["id"].astype(int)
        df["numero"] = df["numero"].astype(int)
        df["ano"] = df["ano"].astype(int)

        df["data_apresentacao"] = pd.to_datetime(
            df["data_apresentacao"], errors="coerce"
        )

        # Datas inválidas
        n_inv = df["data_apresentacao"].isna().sum()
        if n_inv:
            logger.warning(
                f"[proposicoes] {n_inv} registros com data_apresentacao inválida (mantidos com NaT)."
            )

        df["ementa"] = df["ementa"].str.strip()
        df["sigla_tipo"] = df["sigla_tipo"].str.strip().str.upper()

        # Colunas para a Etapa 4
        df["tema"] = pd.NA
        df["resumo_executivo"] = pd.NA

        logger.info(f"[proposicoes] {len(df)} registros prontos.")
        return df

    # ------------------------------------------------------------------
    # fato_votacoes
    # ------------------------------------------------------------------

    def _transform_votacoes(self) -> pd.DataFrame:
        df = self._ler_jsonl("votacoes")

        df = df.rename(
            columns={
                "dataHoraRegistro": "data_hora_registro",
                "siglaOrgao": "sigla_orgao",
                "descricao": "descricao",
                "aprovacao": "aprovacao",
                "proposicaoObjeto": "proposicao_objeto",
                "uriProposicaoObjeto": "uri_proposicao_objeto",
            }
        )[
            [
                "id",
                "data",
                "data_hora_registro",
                "sigla_orgao",
                "descricao",
                "aprovacao",
                "proposicao_objeto",
                "uri_proposicao_objeto",
            ]
        ]

        df = self._validar_nulos(df, ["id", "data"], "votacoes")
        df = self._deduplicar(df, "id", "votacoes")

        # Datas
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
        df["data_hora_registro"] = pd.to_datetime(
            df["data_hora_registro"], errors="coerce"
        )

        df["id_proposicao"] = (
            df["uri_proposicao_objeto"]
            .str.extract(r"/proposicoes/(\d+)$")[0]
            .astype("Int64")
        )

        # aprovacao pode ser 0, 1 ou None
        df["aprovacao"] = df["aprovacao"].astype("Int64")

        logger.info(f"[votacoes] {len(df)} registros prontos.")
        return df

    # ------------------------------------------------------------------
    # fato_despesas
    # ------------------------------------------------------------------

    def _transform_despesas(self) -> pd.DataFrame:
        """
        Combina despesas2025.json e despesas2026.json.
        Ambos têm envelope {'dados': [...]}.
        """
        dfs = []
        for nome in ["despesas2025", "despesas2026"]:
            try:
                dfs.append(self._ler_json_dados(nome))
            except FileNotFoundError:
                logger.warning(f"[despesas] {nome}.json não encontrado — ignorado.")

        if not dfs:
            raise FileNotFoundError(
                "Nenhum arquivo de despesas encontrado em data/raw/. "
                "Coloque despesas2025.json e/ou despesas2026.json lá."
            )

        df = pd.concat(dfs, ignore_index=True)
        logger.info(f"[despesas] {len(df)} registros combinados (2025 + 2026).")

        df = df.rename(
            columns={
                "idDocumento": "id_documento",
                "numeroDeputadoID": "id_deputado",
                "nomeParlamentar": "nome_parlamentar",
                "dataEmissao": "data_emissao",
                "valorDocumento": "valor_documento",
                "valorGlosa": "valor_glosa",
                "valorLiquido": "valor_liquido",
                "descricao": "descricao",
                "descricaoEspecificacao": "descricao_especificacao",
                "fornecedor": "fornecedor",
                "cnpjCPF": "cnpj_cpf",
                "numero": "numero_documento",
                "tipoDocumento": "tipo_documento",
                "mes": "mes",
                "ano": "ano",
                "urlDocumento": "url_documento",
            }
        )[
            [
                "id_documento",
                "id_deputado",
                "nome_parlamentar",
                "data_emissao",
                "valor_documento",
                "valor_glosa",
                "valor_liquido",
                "descricao",
                "descricao_especificacao",
                "fornecedor",
                "cnpj_cpf",
                "numero_documento",
                "tipo_documento",
                "mes",
                "ano",
                "url_documento",
            ]
        ]

        df = self._validar_nulos(df, ["id_documento", "id_deputado"], "despesas")
        df = self._deduplicar(df, "id_documento", "despesas")

        # Tipos
        df["id_documento"] = df["id_documento"].astype(int)
        df["id_deputado"] = df["id_deputado"].astype(int)
        df["mes"] = df["mes"].astype(int)
        df["ano"] = df["ano"].astype(int)
        df["data_emissao"] = pd.to_datetime(df["data_emissao"], errors="coerce")

        # Valores monetários: podem vir como string ou int
        for col in ["valor_documento", "valor_glosa", "valor_liquido"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        # Validação: valor_liquido negativo é dado incorreto
        negativos = (df["valor_liquido"] < 0).sum()
        if negativos:
            logger.warning(
                f"[despesas] {negativos} registros com valor_liquido negativo → removidos."
            )
            df = df[df["valor_liquido"] >= 0].copy()

        # Limpeza de strings
        for col in ["cnpj_cpf", "fornecedor", "descricao", "descricao_especificacao"]:
            df[col] = df[col].str.strip()

        logger.info(f"[despesas] {len(df)} registros prontos.")
        return df

    # ------------------------------------------------------------------
    # Execução
    # ------------------------------------------------------------------

    def run(self) -> dict[str, pd.DataFrame]:
        """
        Executa todas as transformações em sequência.

        Retorna um dicionário:
            {
                "dim_deputados":    DataFrame,
                "dim_partidos":     DataFrame,
                "fato_proposicoes": DataFrame,
                "fato_votacoes":    DataFrame,
                "fato_despesas":    DataFrame,
            }

        Tabelas com falha são omitidas do dicionário; o erro é logado
        sem interromper as demais.
        """
        logger.info("=== Iniciando fase de TRANSFORM ===")

        etapas = [
            ("dim_deputados", self._transform_deputados),
            ("dim_partidos", self._transform_partidos),
            ("fato_proposicoes", self._transform_proposicoes),
            ("fato_votacoes", self._transform_votacoes),
            ("fato_despesas", self._transform_despesas),
        ]

        resultado: dict[str, pd.DataFrame] = {}

        for nome, fn in etapas:
            try:
                df = fn()
                self._salvar_parquet(df, nome)
                resultado[nome] = df
                logger.info(f"[{nome}] ✓  {len(df)} registros.")
            except FileNotFoundError as e:
                logger.error(f"[{nome}] Arquivo ausente: {e}")
            except Exception as e:
                logger.error(f"[{nome}] Falha inesperada: {e}", exc_info=True)

        logger.info(
            f"=== TRANSFORM concluído: {len(resultado)}/{len(etapas)} tabelas geradas ==="
        )
        return resultado
