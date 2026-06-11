# Documentação Técnica — Pipeline ETL Bússola Pública

## Sumário

1. [Visão geral da arquitetura](#1-visão-geral-da-arquitetura)
2. [Etapa 1 — Extract](#2-etapa-1--extract)
3. [Etapa 2 — Transform](#3-etapa-2--transform)
4. [Etapa 3 — Load](#4-etapa-3--load)
5. [Modelo de dados](#5-modelo-de-dados)
6. [Orquestração](#6-orquestração)
7. [Decisões transversais](#7-decisões-transversais)

---

## 1. Visão geral da arquitetura

O pipeline segue o padrão clássico de **ETL em três fases desacopladas**, cada uma com responsabilidade única e ponto de persistência próprio:

```
API da Câmara
     │
     ▼
[ Extract ]  →  data/raw/*.jsonl         (JSON bruto, uma linha por registro)
     │
     ▼
[ Transform ] →  data/processed/*.parquet  (DataFrames limpos e tipados)
     │
     ▼
[ Load ]     →  PostgreSQL / Supabase     (tabelas prontas para consulta)
```

### Por que três fases com persistência intermediária?

Cada fase grava seu resultado antes de passar para a próxima. Isso garante três propriedades:

**Rerunnabilidade parcial.** Se o Transform falhar após uma extração demorada, basta rodar `python main.py --fase transform` — a API não é chamada novamente. O mesmo vale para o Load: `python main.py --fase load` reutiliza os parquets sem reprocessar nada.

**Rastreabilidade.** Os arquivos intermediários funcionam como snapshot do estado em cada ponto do pipeline. Se o banco apresentar dados errados, é possível inspecionar o parquet e o JSONL para determinar em qual fase o problema ocorreu.

**Separação de custos.** Chamadas de API têm latência. Transformações têm custo de CPU. Cargas têm custo de conexão de rede. Desacoplar as fases permite otimizar cada uma independentemente.

### Estrutura de arquivos

```
projeto/
├── main.py               # Orquestrador com argparse
├── src/
│   ├── extract.py        # ClientAPI — coleta da API da Câmara
│   ├── transform.py      # Transformer — validação e modelagem
│   └── load.py           # Loader — carga no PostgreSQL
├── core/
│   └── exceptions.py     # APIRateLimitError, APIConnectionError
├── data/
│   ├── raw/              # JSONLs brutos (saída do Extract)
│   └── processed/        # Parquets limpos (saída do Transform)
└── .env                  # DATABASE_URL (não versionado)
```

---

## 2. Etapa 1 — Extract

**Arquivo:** `src/extract.py`  
**Classe:** `ClientAPI`  
**Saída:** `data/raw/{endpoint}.jsonl`

### 2.1 Endpoints extraídos

| Arquivo gerado        | Endpoint API              | Filtro de data | Volume esperado     |
|-----------------------|---------------------------|----------------|---------------------|
| `deputados.jsonl`     | `/deputados`              | Não            | 513 registros fixos |
| `partidos.jsonl`      | `/partidos`               | Não            | ~21 registros fixos |
| `proposicoes.jsonl`   | `/proposicoes`            | Sim (90 dias)  | Variável por janela |
| `votacoes.jsonl`      | `/votacoes`               | Sim (90 dias)  | Variável por janela |

### 2.2 Paginação via HATEOAS

A API da Câmara retorna no máximo 100 itens por resposta e inclui um campo `links` com `rel="next"` apontando para a página seguinte. O método `extrator_api` implementa essa navegação como um gerador Python:

```python
while url:
    data = self._get_page(url, params)
    yield data
    params = None   # absorvidos pelo link HATEOAS na próxima iteração
    url = next((l["href"] for l in data["links"] if l["rel"] == "next"), None)
```

O detalhe crítico está na linha `params = None`: após a primeira requisição, os parâmetros já estão embutidos na URL `next` devolvida pela API. Reenviá-los causaria conflito ou duplicação de filtros.

### 2.3 Janelas de 90 dias

A API rejeita intervalos maiores que 3 meses para `proposicoes` e `votacoes` com HTTP 400 e mensagem `"A diferença entre as datas não pode ser maior que 3 meses"`. A função `gerar_janelas` em `main.py` quebra o intervalo 2025-01-01 → hoje em fatias de exatamente 89 dias (max_dias=90 → timedelta(89)), gerando cerca de 6 janelas para cobrir todo o ano de 2025 até meados de 2026.

Cada janela é uma entrada separada em `params_list`, e `processar_endpoint` as itera sequencialmente, acumulando os registros no mesmo arquivo. O arquivo só é deletado uma vez, antes da primeira janela — não entre elas.

### 2.4 Retry com backoff exponencial

O decorator `@retry` da biblioteca `tenacity` é aplicado sobre `_get_page`:

```python
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=3, max=30),
    reraise=True,
)
```

A sequência de espera é: **3s → 6s → 12s** (capped em 30s). Após três tentativas com falha, a exceção original é relançada com `reraise=True`.

**O que é retentável e o que não é:**

| Status HTTP | Comportamento                                               |
|-------------|-------------------------------------------------------------|
| `429`       | Levanta `APIRateLimitError` — retentável pelo decorator      |
| `400/401/403/404` | Levanta `APIConnectionError` — **não retentável** (erro de configuração, não de rede) |
| Timeout     | `requests.Timeout` — retentável pelo decorator              |
| `5xx`       | `raise_for_status()` → `HTTPError` — retentável pelo decorator |

Erros 4xx não são retentados porque são determinísticos: tentar novamente com os mesmos parâmetros produzirá o mesmo erro. Retentar seria desperdício de tempo e requisições.

### 2.5 Paralelismo com ThreadPoolExecutor

```python
with ThreadPoolExecutor(max_workers=3) as executor:
    futuros = {executor.submit(self.processar_endpoint, ep, params_list): ep
               for ep, params_list in endpoints}
```

O limite de **3 workers** não é arbitrário: com 4 endpoints sendo extraídos e `deputados`/`partidos` terminando em poucos segundos, raramente há mais de 2-3 threads ativas simultaneamente. Mais workers aumentariam o risco de hit no rate limit de uma API pública sem ganho real de velocidade.

Cada thread escreve em seu próprio arquivo, portanto não há concorrência de escrita — o paralelismo é seguro sem locks.

### 2.6 Formato JSONL

Os registros são salvos em JSONL (JSON Lines) — um objeto JSON por linha — em vez de um array JSON:

```
{"id": 204379, "nome": "Acácio Favacho", ...}
{"id": 160511, "nome": "Abou Anni", ...}
```

Vantagens sobre JSON convencional:
- **Append incremental**: basta abrir com `mode="a"` e escrever linha a linha, sem carregar o arquivo inteiro em memória.
- **Leitura eficiente**: `pd.read_json(path, lines=True)` no Transform processa o arquivo em stream.
- **Resiliência parcial**: um arquivo truncado por crash ainda tem todas as linhas anteriores válidas.

### 2.7 Idempotência

O arquivo é deletado no início de cada execução de `processar_endpoint`:

```python
if caminho_arquivo.exists():
    caminho_arquivo.unlink()
```

Isso garante que rodar o Extract duas vezes no mesmo dia não duplica registros. A operação é feita uma única vez antes da primeira janela — deletar entre janelas destruiria os dados já coletados.

---

## 3. Etapa 2 — Transform

**Arquivo:** `src/transform.py`  
**Classe:** `Transformer`  
**Entrada:** `data/raw/*.jsonl` e `data/raw/despesas*.json`  
**Saída:** `data/processed/*.parquet` + dicionário de DataFrames

### 3.1 Estratégia geral de validação

Cada entidade passa por três verificações antes de qualquer transformação:

1. **Nulos em campos obrigatórios** — `_validar_nulos(df, campos, entidade)`: remove registros onde os campos listados são nulos e loga quantos foram removidos. Campos obrigatórios são aqueles sem os quais o registro não pode ser identificado ou carregado (PK e campos usados em junções).

2. **Deduplicação pela chave primária** — `_deduplicar(df, pk, entidade)`: remove linhas duplicadas pelo campo PK. A API pode retornar o mesmo registro em janelas de data diferentes quando uma proposição foi atualizada.

3. **Validações específicas por entidade** — aplicadas após as anteriores, documentadas por entidade abaixo.

A filosofia adotada é **remover e logar, não falhar**: registros inválidos são descartados com aviso, não levantam exceção. O pipeline completa e o engenheiro analisa os logs para entender a qualidade dos dados.

### 3.2 Transformações por entidade

#### deputados → `dim_deputados`

Fonte: `data/raw/deputados.jsonl` (513 registros, flat JSON)

| Campo original   | Campo final      | Transformação                          |
|------------------|------------------|----------------------------------------|
| `id`             | `id`             | `astype(int)`                          |
| `nome`           | `nome`           | `str.strip()`                          |
| `siglaPartido`   | `sigla_partido`  | `str.strip().str.upper()`              |
| `siglaUf`        | `sigla_uf`       | `str.strip().str.upper()`              |
| `idLegislatura`  | `id_legislatura` | `astype(int)`                          |
| `urlFoto`        | `url_foto`       | mantido como string                    |
| `email`          | `email`          | `str.strip().str.lower()`              |

Campos descartados: `uri`, `uriPartido` — são URLs de referência à própria API, sem valor analítico.

Campos obrigatórios para validação de nulos: `id`, `nome`.

#### partidos → `dim_partidos`

Fonte: `data/raw/partidos.jsonl` (21 registros, flat JSON)

| Campo original | Campo final | Transformação             |
|----------------|-------------|---------------------------|
| `id`           | `id`        | `astype(int)`             |
| `sigla`        | `sigla`     | `str.strip().str.upper()` |
| `nome`         | `nome`      | `str.strip()`             |
| `uri`          | `uri`       | mantido como string       |

A tabela de partidos é pequena e estável. Mantida completa sem filtros adicionais.

#### proposicoes → `fato_proposicoes`

Fonte: `data/raw/proposicoes.jsonl` (estrutura flat JSON)

| Campo original     | Campo final          | Transformação                            |
|--------------------|----------------------|------------------------------------------|
| `id`               | `id`                 | `astype(int)`                            |
| `siglaTipo`        | `sigla_tipo`         | `str.strip().str.upper()`                |
| `codTipo`          | `cod_tipo`           | `astype(int)`                            |
| `numero`           | `numero`             | `astype(int)`                            |
| `ano`              | `ano`                | `astype(int)`                            |
| `ementa`           | `ementa`             | `str.strip()`                            |
| `dataApresentacao` | `data_apresentacao`  | `pd.to_datetime(..., errors="coerce")`   |
| `uri`              | `uri`                | mantido como string                      |
| —                  | `tema`               | `pd.NA` (reservado para Etapa 4)         |
| —                  | `resumo_executivo`   | `pd.NA` (reservado para Etapa 4)         |

**Decisão sobre datas históricas:** a API com `dataInicio=2025-01-01` retorna proposições cuja _tramitação_ está ativa em 2025, mas cuja _apresentação_ pode datar de 1989. O filtro da API age sobre a data de movimentação, não de apresentação. Por isso, `data_apresentacao` com valores históricos é mantida como válida. Datas efetivamente inválidas (não parseáveis) recebem `NaT` e são logadas como aviso — não removidas, para não perder a proposição completa por causa de um campo acessório.

**Ementa como campo obrigatório:** registros sem ementa são removidos. A ementa é a entrada da Etapa 4 (classificação por embeddings e resumo executivo via LLM). Sem ela, o registro não tem utilidade para o produto.

**Colunas reservadas para a IA:** `tema` e `resumo_executivo` são criadas vazias. Quando a Etapa 4 rodar, ela fará apenas `UPDATE fato_proposicoes SET tema = ?, resumo_executivo = ? WHERE id = ?` sem precisar reprocessar toda a tabela.

#### votacoes → `fato_votacoes`

Fonte: `data/raw/votacoes.jsonl` (459 registros, flat JSON)

| Campo original          | Campo final             | Transformação                                      |
|-------------------------|-------------------------|----------------------------------------------------|
| `id`                    | `id`                    | mantido como string (formato `"2442848-52"`)       |
| `data`                  | `data`                  | `pd.to_datetime(..., errors="coerce")`             |
| `dataHoraRegistro`      | `data_hora_registro`    | `pd.to_datetime(..., errors="coerce")`             |
| `siglaOrgao`            | `sigla_orgao`           | mantido como string                                |
| `descricao`             | `descricao`             | mantido como string                                |
| `aprovacao`             | `aprovacao`             | `astype("Int64")` — nullable integer               |
| `proposicaoObjeto`      | `proposicao_objeto`     | mantido como string (nome textual)                 |
| `uriProposicaoObjeto`   | `uri_proposicao_objeto` | mantido como string                                |
| —                       | `id_proposicao`         | extraído via regex da URI (veja abaixo)            |

**Por que `id` é string?** O identificador de votação no formato `"2442848-52"` (número da sessão + número sequencial) não é um inteiro. Converter para inteiro exigiria uma tabela auxiliar de sessões ou concatenação artificial. Mantê-lo como string preserva o formato original e evita colisões entre votações de sessões diferentes.

**`aprovacao` como `Int64` nullable:** o campo pode ser `0` (rejeitado), `1` (aprovado) ou `None` (votação não conclusiva ou sem registro de resultado). O tipo `Int64` do pandas suporta inteiros nulos nativamente — diferente de `int64`, que armazena `None` como `float('nan')`, causando erro na carga.

**Extração de `id_proposicao` via regex:**
```python
df["id_proposicao"] = (
    df["uri_proposicao_objeto"]
    .str.extract(r"/proposicoes/(\d+)$")[0]
    .astype("Int64")
)
```
O campo `proposicaoObjeto` contém o nome textual da proposição (`"REQ 10/2025 CPASF"`), não o ID numérico. O ID real está embutido na URI (`".../proposicoes/2488420"`). O regex captura o número ao final da URI. Como 348 das 459 votações (75,8%) não têm proposição vinculada, `id_proposicao` é `pd.NA` para a maioria dos registros.

#### despesas → `fato_despesas`

Fonte: `data/raw/despesas2025.json` e `data/raw/despesas2026.json`

**Diferença estrutural em relação aos demais:** os arquivos de despesas não são JSONL. São JSONs completos com um envelope `{"dados": [...]}` — padrão dos arquivos baixados diretamente do portal da transparência da Câmara. O método `_ler_json_dados` detecta esse envelope automaticamente:

```python
registros = raw.get("dados", raw) if isinstance(raw, dict) else raw
```

Se não houver envelope (lista raiz), trata diretamente como lista. Isso torna o método robusto a variações do formato de exportação.

| Campo original           | Campo final              | Transformação                              |
|--------------------------|--------------------------|--------------------------------------------|
| `idDocumento`            | `id_documento`           | `astype(int)`                              |
| `numeroDeputadoID`       | `id_deputado`            | `astype(int)`                              |
| `nomeParlamentar`        | `nome_parlamentar`       | `str.strip()`                              |
| `dataEmissao`            | `data_emissao`           | `pd.to_datetime(..., errors="coerce")`     |
| `valorDocumento`         | `valor_documento`        | `pd.to_numeric(..., errors="coerce")`      |
| `valorGlosa`             | `valor_glosa`            | `pd.to_numeric(..., errors="coerce")`      |
| `valorLiquido`           | `valor_liquido`          | `pd.to_numeric(..., errors="coerce")`      |
| `descricao`              | `descricao`              | `str.strip()`                              |
| `descricaoEspecificacao` | `descricao_especificacao`| `str.strip()`                              |
| `fornecedor`             | `fornecedor`             | `str.strip()`                              |
| `cnpjCPF`                | `cnpj_cpf`               | `str.strip()`                              |
| `numero`                 | `numero_documento`       | mantido como string                        |
| `tipoDocumento`          | `tipo_documento`         | mantido como string                        |
| `mes`                    | `mes`                    | `astype(int)`                              |
| `ano`                    | `ano`                    | `astype(int)`                              |
| `urlDocumento`           | `url_documento`          | mantido como string                        |

**Por que `pd.to_numeric` para valores monetários?** Os campos de valor chegam inconsistentes: `1467` (inteiro), `"265.6"` (string com decimal) e eventualmente `""` (string vazia em documentos sem valor). `pd.to_numeric(..., errors="coerce")` converte tudo para float, coercindo strings inválidas para `NaN`, que em seguida é preenchido com `0.0` via `.fillna(0.0)`.

**Filtro de valores negativos:** `valor_liquido < 0` é removido. No contexto de cota parlamentar, um valor líquido negativo indica inconsistência contábil — possivelmente um estorno registrado de forma incorreta. O registro é logado e descartado em vez de ser corrigido automaticamente.

**`cnpjCPF` com espaço trailing:** o campo vem com espaço ao final (`"085.324.290/0013-1 "`). O `str.strip()` resolve isso. Sem essa limpeza, queries de agrupamento por CNPJ retornariam resultados incorretos com registros aparentemente distintos para o mesmo fornecedor.

### 3.3 Checkpointing em Parquet

Após cada transformação bem-sucedida, o DataFrame é salvo em `data/processed/{nome}.parquet`. O Parquet foi escolhido sobre CSV por três razões:

1. **Tipos preservados:** datas, inteiros nullable (`Int64`) e floats são serializados com fidelidade. Um CSV serializa tudo como string, exigindo reparse no Load.
2. **Performance de leitura:** colunas não utilizadas são puladas sem ler o arquivo inteiro — útil quando o Load precisa apenas de um subconjunto de colunas.
3. **Tamanho:** compressão snappy embutida reduz o espaço em disco em 60-80% comparado a CSV para dados textuais como ementas.

---

## 4. Etapa 3 — Load

**Arquivo:** `src/load.py`  
**Classe:** `Loader`  
**Entrada:** dicionário `{nome: DataFrame}` retornado pelo Transform  
**Saída:** tabelas populadas no PostgreSQL (Supabase)

### 4.1 Conexão com o banco

A connection string é lida do `.env` via `os.getenv("DATABASE_URL")`. A ausência da variável levanta `EnvironmentError` imediatamente no `__init__` — antes de qualquer operação, com mensagem clara de como corrigi-la.

```python
DATABASE_URL=postgresql://postgres:[SENHA]@db.[REF].supabase.co:5432/postgres?sslmode=require
```

O `?sslmode=require` é obrigatório para o Supabase — conexões sem SSL são recusadas. O engine é criado com `pool_pre_ping=True`, que executa um `SELECT 1` antes de entregar cada conexão do pool — evita erros silenciosos quando a conexão ficou ociosa por tempo suficiente para o servidor fechar o socket.

### 4.2 SQLAlchemy Core vs ORM

O Loader usa **SQLAlchemy Core** (objetos `Table`, `Column`, `insert`) em vez do ORM (classes mapeadas com `Base.metadata`). A razão é operacional: o Core permite construir statements de inserção em batch diretamente sobre listas de dicts, sem instanciar um objeto Python por registro. Para tabelas com dezenas de milhares de linhas, a diferença de performance é significativa.

O ORM seria adequado para aplicações que leem e modificam objetos individualmente — não para carga em massa de pipeline.

### 4.3 Upsert com `ON CONFLICT DO UPDATE`

O método `_upsert` usa a cláusula nativa do PostgreSQL via SQLAlchemy:

```python
stmt = insert(tabela).values(batch)
stmt = stmt.on_conflict_do_update(
    index_elements=[pk],
    set_={col: insert(tabela).excluded[col] for col in colunas_atualizaveis},
)
```

`EXCLUDED` é uma pseudo-tabela do PostgreSQL que referencia os valores que seriam inseridos mas foram bloqueados pelo conflito. `set_={"nome": excluded.nome}` significa "se já existe um registro com esse id, atualize o campo nome com o valor que tentamos inserir".

Isso torna o pipeline **idempotente**: rodar duas vezes no mesmo dia não duplica nem falha — atualiza os registros com os valores mais recentes.

**Quando o `on_conflict_do_update` não tem colunas para atualizar** (tabela de uma coluna só), o método cai para `on_conflict_do_nothing` automaticamente.

### 4.4 Batching

Os registros são enviados em lotes de 500 (constante `BATCH_SIZE`):

```python
for i in range(0, len(registros), BATCH_SIZE):
    batch = registros[i : i + BATCH_SIZE]
    conn.execute(stmt.values(batch))
```

O limite de 500 foi escolhido considerando o plano gratuito do Supabase, que tem timeout de conexão curto e limite de statement size. Lotes maiores (ex: 5.000) causam timeout intermitente para tabelas grandes como `fato_despesas`. Lotes menores (ex: 50) aumentam o número de round-trips sem ganho real de robustez.

Toda a carga de uma tabela roda dentro de um único `with self.engine.begin() as conn:`, garantindo que ou todos os batches são commitados ou nenhum — transação atômica por tabela.

### 4.5 Conversão de NaN para None

```python
df.astype(object).where(pd.notna(df), other=None).to_dict(orient="records")
```

Esta conversão resolve um problema específico do pandas com PostgreSQL: o pandas usa `float('nan')` internamente para representar valores ausentes em colunas de tipo inteiro ou objeto. O psycopg2 (driver PostgreSQL) rejeita `float('nan')` com `DataError: invalid input syntax for type integer`. A substituição por `None` explícito é interpretada como `NULL` pelo driver.

O `.astype(object)` é necessário porque `Int64` nullable do pandas serializa inteiros nulos como `<NA>` (tipo `pd.NA`), não `None` — e `pd.NA` também causa erro no driver. Converter para `object` primeiro garante que tudo que `pd.notna()` marca como falso vira `None` Python.

### 4.6 Ausência de Foreign Keys no banco

As relações lógicas entre tabelas existem (despesas → deputados, votações → proposições) mas não são enforçadas como constraints `FOREIGN KEY` no PostgreSQL. A razão é a natureza incremental do pipeline:

- Uma votação do dia 15 pode referenciar uma proposição que só será extraída quando a janela do dia 16 rodar.
- Um deputado pode ter despesas registradas antes de sua primeira votação ser extraída.

Com FKs enforçadas, essas situações causariam `ForeignKeyViolationError` e interromperiam a carga. Sem elas, a integridade referencial é garantida pela **ordem de carga** (dimensões antes dos fatos) e pela remoção de registros com campos obrigatórios nulos no Transform.

### 4.7 Ordem de carga

```
1. dim_partidos      (sem dependências)
2. dim_deputados     (sem dependências)
3. fato_proposicoes  (sem dependências de FK)
4. fato_votacoes     (referencia proposições logicamente)
5. fato_despesas     (referencia deputados logicamente)
```

Mesmo sem FKs enforçadas, a ordem é mantida como boa prática: se FKs forem adicionadas no futuro para análise de integridade, a carga funcionará sem alterações.

### 4.8 Verificação pós-carga

Após cada `_load_X`, o método `_contar_registros` executa `SELECT COUNT(*) FROM {tabela}` e loga o resultado. Isso serve como sanidade check imediata — se a tabela tem 0 registros após a carga, algo silenciosamente deu errado.

---

## 5. Modelo de dados

### 5.1 Diagrama de tabelas

```
dim_partidos                    dim_deputados
─────────────────               ──────────────────────────
id          INTEGER PK          id             INTEGER PK
sigla       VARCHAR(20)         nome           VARCHAR(255)
nome        VARCHAR(255)        sigla_partido  VARCHAR(20)
uri         TEXT                sigla_uf       VARCHAR(2)
                                id_legislatura INTEGER
                                url_foto       TEXT
                                email          VARCHAR(255)

fato_proposicoes                fato_votacoes
──────────────────────────      ──────────────────────────
id               INTEGER PK     id                    VARCHAR(50) PK
sigla_tipo       VARCHAR(20)    data                  DATE
cod_tipo         INTEGER        data_hora_registro    TIMESTAMP
numero           INTEGER        sigla_orgao           VARCHAR(50)
ano              INTEGER        descricao             TEXT
ementa           TEXT           aprovacao             INTEGER (0/1/NULL)
data_apresentacao TIMESTAMP     proposicao_objeto     VARCHAR(255)
uri              TEXT           uri_proposicao_objeto TEXT
tema             VARCHAR(100) ← id_proposicao         BIGINT ──┐ FK lógica
resumo_executivo TEXT ←──────── Etapa 4               └────────┘

fato_despesas
──────────────────────────────
id_documento          BIGINT PK
id_deputado           INTEGER ────── FK lógica → dim_deputados.id
nome_parlamentar      VARCHAR(255)
data_emissao          TIMESTAMP
valor_documento       FLOAT
valor_glosa           FLOAT
valor_liquido         FLOAT
descricao             VARCHAR(255)
descricao_especificacao VARCHAR(255)
fornecedor            VARCHAR(255)
cnpj_cpf              VARCHAR(30)
numero_documento      VARCHAR(100)
tipo_documento        VARCHAR(10)
mes                   INTEGER
ano                   INTEGER
url_documento         TEXT
```

### 5.2 Colunas reservadas para a Etapa 4

`fato_proposicoes.tema` e `fato_proposicoes.resumo_executivo` são criadas nesta etapa já vazias (`NULL`). A Etapa 4 (IA) fará apenas `UPDATE` nessas colunas — sem recriar a tabela, sem reprocessar o pipeline inteiro. Essa decisão de design antecipa o próximo passo e evita uma migração de schema posterior.

---

## 6. Orquestração

**Arquivo:** `main.py`

### 6.1 Modos de execução

```bash
python main.py               # Executa tudo: extract → transform → load
python main.py --fase extract    # Apenas coleta da API
python main.py --fase transform  # Apenas transformação (lê data/raw/)
python main.py --fase load       # Apenas carga (lê data/processed/)
```

O modo `--fase load` isolado lê os parquets de `data/processed/` sem passar pelo Transform. Isso permite recarregar o banco após uma correção de schema ou uma limpeza manual sem recoletar da API.

### 6.2 Tratamento de erros entre fases

```python
def _executar_fase(nome, fn, logger, **kwargs):
    try:
        resultado = fn(logger, **kwargs)
        return resultado
    except Exception as e:
        logger.error(f"[{nome}] falhou: {e}", exc_info=True)
        return None
```

Falhas em uma fase são logadas mas não relançadas no modo `tudo`. Se o Extract falhar parcialmente (ex: timeout na janela 4 de 6), o Transform ainda roda com os dados disponíveis em `data/raw/` e o Load popula o banco com o que existe. O engenheiro analisa os logs e roda `--fase extract` para completar a extração.

### 6.3 Timing por fase

Cada fase é cronometrada com `time.perf_counter()`:

```
18:34:59 [INFO] Pipeline Bússola Pública iniciado — fase: 'tudo'
18:34:59 [INFO] === Fase 1: EXTRACT ===
...
18:38:12 [INFO] [Extract] concluído em 193.4s.
18:38:12 [INFO] === Fase 2: TRANSFORM ===
...
18:38:15 [INFO] [Transform] concluído em 3.1s.
18:38:15 [INFO] === Fase 3: LOAD ===
...
18:38:47 [INFO] [Load] concluído em 31.8s.
18:38:47 [INFO] Pipeline concluído em 228.3s.
```

---

## 7. Decisões transversais

### 7.1 Logging estruturado

Todo o pipeline usa o módulo `logging` padrão do Python com o formato:
```
HH:MM:SS [NIVEL] mensagem
```

Não foi usada nenhuma biblioteca de logging estruturado (structlog, loguru) para manter zero dependências externas além das funcionais. O padrão `[entidade] mensagem` nos logs permite filtrar por endpoint ou tabela com `grep` em produção.

### 7.2 Sem frameworks ETL externos

O pipeline não usa Airflow, Luigi, Prefect ou similar. A decisão é intencional para um projeto de portfólio: frameworks de orquestração adicionam complexidade de infraestrutura (servidores, bancos de metadados, UIs) que ofusca o código real. A orquestração via `main.py` com `argparse` é suficiente para a escala atual e compreensível por qualquer recrutador que abrir o repositório.

### 7.3 Variáveis de ambiente via `.env`

Credenciais nunca são hardcoded. O `python-dotenv` carrega o `.env` no início do `load.py` com `load_dotenv()`. O arquivo `.env` deve estar no `.gitignore`:

```gitignore
.env
data/raw/
data/processed/
```

`data/raw/` e `data/processed/` também devem ser ignorados — são artefatos gerados pelo pipeline, não código-fonte.

### 7.4 Compatibilidade com o n8n (Etapa 5)

O n8n tem integração nativa com PostgreSQL. Os workflows da Etapa 5 podem conectar diretamente ao Supabase e executar queries sobre as tabelas criadas aqui. A estrutura de nomes (`dim_*`, `fato_*`) e a presença das colunas `tema` e `resumo_executivo` já preparadas facilitam a construção de alertas e relatórios automatizados sem alterações de schema.