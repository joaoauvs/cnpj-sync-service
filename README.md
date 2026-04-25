# CNPJ Sync Service

Serviço de sincronização dos dados públicos de CNPJ da Receita Federal para PostgreSQL.

Baixa, extrai, normaliza e carrega ~196 milhões de registros via `COPY FROM STDIN` com upsert idempotente. Suporta reuso de artefatos entre execuções e rastreamento de progresso por arquivo.

## Fluxo Resumido

```
Descoberta de snapshot → Download paralelo → Extração → Processamento → Carga no PostgreSQL
```

1. Descobre o snapshot mais recente via WebDAV da Receita Federal (fallback: Casa dos Dados)
2. Reutiliza ZIPs em `data/downloads/` e artefatos em `data/processed/` quando válidos
3. Extrai ZIPs e normaliza CSVs (datas, decimais, encoding latin-1)
4. Carrega no PostgreSQL via `COPY FROM STDIN` + `INSERT … ON CONFLICT … DO UPDATE`
5. Registra progresso em `cnpj.controle_sincronizacao` e `cnpj.controle_arquivos`

## Pré-requisitos

- Python 3.11+
- PostgreSQL 14+ acessível pela rede

## Instalação

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

## Configuração

Crie `.env` na raiz (veja `.env.example`):

```env
# Conexão PostgreSQL — use URL completa (prioridade) ou variáveis individuais
# DATABASE_URL=postgresql://usuario:senha@host:5432/banco
DB_SERVER=seu-host
DB_DATABASE=seu-banco
DB_USERNAME=seu-usuario
DB_PASSWORD=sua-senha

# Comportamento
LOG_LEVEL=INFO           # DEBUG | INFO | WARNING | ERROR
FORCE_SYNC=false         # true → reprocessa mesmo snapshot já carregado
REUSE_PROCESSED=true     # true → reutiliza data/processed/ entre execuções
SNAPSHOT_DATE=           # vazio → snapshot mais recente; ex: 2026-04 ou 2026-04-01
```

## Execução

Via variáveis de ambiente ou flags CLI (ambas funcionam; flags têm prioridade):

```bash
# Sincronização completa com o snapshot mais recente
python main.py

# Forçar reprocessamento do mesmo snapshot
python main.py --force
set FORCE_SYNC=true && python main.py        # Windows (env var)
FORCE_SYNC=true python main.py              # Linux/macOS

# Snapshot específico (YYYY-MM ou YYYY-MM-DD)
python main.py --date 2026-04
set SNAPSHOT_DATE=2026-04 && python main.py

# Nível de log detalhado
python main.py --log-level DEBUG
set LOG_LEVEL=DEBUG && python main.py

# Ajustar paralelismo
python main.py --workers 8                  # download + processamento
python main.py --download-workers 12 --process-workers 4

# Conexão ao banco via CLI
python main.py --server host --database banco --username user --password senha

# Ver todas as opções
python main.py --help
```

## Estrutura do Projeto

```
cnpj-sync-service/
├── main.py                  # Entrypoint — CNPJSyncApplication
├── requirements.txt
├── requirements-dev.txt     # Dependências de desenvolvimento (pytest)
├── pytest.ini               # Configuração do pytest
├── .env.example
├── sql/
│   └── schema.sql           # DDL idempotente (PostgreSQL 14+) + view
├── src/
│   ├── config.py            # Todas as constantes configuráveis
│   ├── crawler.py           # Descoberta de snapshot (WebDAV / HTML fallback)
│   ├── database.py          # Operações PostgreSQL (COPY, upserts, controle)
│   ├── downloader.py        # Download paralelo com resume e retry
│   ├── extractor.py         # Extração de ZIPs
│   ├── logger_enhanced.py   # Logger estruturado (Loguru)
│   ├── models.py            # Modelos Pydantic v2 do pipeline
│   ├── pipeline.py          # Orquestração download → extract → process
│   ├── processor.py         # Normalização de CSVs → parquet/csv processado
│   ├── storage.py           # Writers plugáveis (CSV ou Parquet)
│   └── sync.py              # CNPJSync — coordena pipeline + carga no banco
├── tests/
│   ├── conftest.py          # Fixtures compartilhadas (conexão DB, cursor)
│   ├── test_config.py       # Schemas, constantes, diretórios
│   ├── test_connection.py   # Conectividade e existência de tabelas no banco
│   ├── test_normalizer.py   # Normalização de datas e decimais (unitário)
│   ├── test_schema.py       # Estrutura de colunas das tabelas e view
│   └── test_view.py         # Integridade de dados via vw_empresas_completo
├── data/
│   ├── downloads/           # ZIPs baixados (preservados entre execuções)
│   ├── extracted/           # CSVs extraídos (removidos após processamento)
│   └── processed/           # Artefatos normalizados (parquet/csv)
├── logs/                    # Logs por execução
└── docs/                    # Documentação técnica detalhada
```

## Arquitetura

```
CNPJSyncApplication (main.py)
  └── CNPJSync (sync.py)
        ├── SnapshotCrawler      → descobre snapshot
        ├── CNPJPipeline
        │     ├── FileDownloader → download paralelo (12 workers)
        │     ├── ZipExtractor   → extração (4 workers)
        │     └── CSVProcessor   → normalização (4 workers)
        └── CNPJDatabase         → COPY + upsert no PostgreSQL
```

Veja [`docs/architecture.md`](docs/architecture.md) para detalhe completo.

## Banco de Dados

Schema criado a partir de `sql/schema.sql` (idempotente). Todas as tabelas no schema `cnpj`:

| Tabela | Tipo | Linhas (aprox.) |
|---|---|---|
| `empresas` | principal | ~60 M |
| `estabelecimentos` | principal | ~71 M |
| `socios` | principal | ~25 M |
| `simples` | principal | ~40 M |
| `cnaes` / `motivos` / `municipios` / `naturezas` / `paises` / `qualificacoes` | referência | < 10 K cada |
| `controle_sincronizacao` | controle idempotência | 1 por execução |
| `controle_arquivos` | controle por arquivo | 1 por ZIP processado |

#### Idempotência

Cada execução registra um estado em `cnpj.controle_sincronizacao`:

| Status | Significa |
|---|---|
| `EM_EXECUCAO` | Sync em andamento (previne execuções concorrentes) |
| `SUCESSO` | Snapshot já carregado — próxima execução pula automaticamente |
| `FALHA` | Execução anterior falhou — será retentada |

Use `--force` / `FORCE_SYNC=true` para reprocessar um snapshot com status `SUCESSO`.

#### View

| View | Descrição |
|---|---|
| `cnpj.vw_empresas_completo` | Join completo: estabelecimentos + empresas + referências + simples. Inclui `cnpj_completo` (14 dígitos), `cnpj_formatado` (XX.XXX.XXX/XXXX-XX) e descrições decodificadas de situação, porte, CNAE, município e país. |

Veja [`docs/database.md`](docs/database.md) para o schema completo.

## Testes

Instale as dependências de desenvolvimento e execute com `pytest`:

```bash
pip install -r requirements-dev.txt

# Suite completa
pytest

# Apenas testes unitários (sem banco de dados)
pytest tests/test_config.py tests/test_normalizer.py

# Testes de integração (requerem .env configurado)
pytest tests/test_connection.py tests/test_schema.py tests/test_view.py
```

| Arquivo de teste | Tipo | Requer banco | Cobertura principal |
|---|---|---|---|
| `test_config.py` | unitário | não | schemas, constantes, diretórios |
| `test_normalizer.py` | unitário | não | normalização de datas e decimais |
| `test_crawler.py` | unitário | não | parsing de nomes, tamanhos, datas, seleção de snapshot, HTML |
| `test_downloader.py` | unitário | não | validação de arquivos locais, ZIP, tolerância de tamanho |
| `test_extractor.py` | unitário | não | extração de ZIPs, corrupção, skip, overwrite |
| `test_storage.py` | unitário | não | CSV e Parquet writers, factory |
| `test_database_helpers.py` | unitário | não | `_df_to_copy_buffer`, null handling, escape de caracteres |
| `test_processor.py` | unitário | não | detecção de grupo, normalização, rejeitos, pipeline CSV→parquet |
| `test_connection.py` | integração | sim | conectividade e tabelas |
| `test_schema.py` | integração | sim | colunas de tabelas e view |
| `test_view.py` | integração | sim | integridade de dados via `vw_empresas_completo` |

```bash
# Apenas unitários (sem banco)
pytest tests/test_config.py tests/test_normalizer.py tests/test_crawler.py \
       tests/test_downloader.py tests/test_extractor.py tests/test_storage.py \
       tests/test_database_helpers.py tests/test_processor.py
```

> Os testes de integração são ignorados automaticamente (`pytest.skip`) se o banco não estiver acessível.

## Configurações de Performance

Em `src/config.py`:

```python
DOWNLOAD_WORKERS = 12      # threads de download (limitado pela rede)
EXTRACT_WORKERS  = 4       # threads de extração (limitado por disco)
PROCESS_WORKERS  = 4       # threads de processamento (limitado por CPU)
CSV_CHUNK_ROWS   = 200_000 # linhas por chunk pandas (controle de memória)
STORAGE_BACKEND  = "parquet"  # "csv" ou "parquet"
```

## Documentação

| Documento | Conteúdo |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Arquitetura detalhada e fluxo de dados |
| [`docs/database.md`](docs/database.md) | Schema SQL e estratégia de carga |
| [`docs/configuration.md`](docs/configuration.md) | Todas as variáveis de ambiente e constantes |
| [`docs/data-pipeline.md`](docs/data-pipeline.md) | Etapas de processamento e normalização |
| [`docs/troubleshooting.md`](docs/troubleshooting.md) | Erros conhecidos e soluções |

## Fontes de Dados

- Fonte primária: [arquivos.receitafederal.gov.br](https://arquivos.receitafederal.gov.br) (WebDAV)
- Fallback: [dados-abertos-rf-cnpj.casadosdados.com.br](https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/)
