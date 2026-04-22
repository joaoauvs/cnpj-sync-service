# CNPJ Sync Service

Serviço de sincronização dos dados públicos de CNPJ da Receita Federal para PostgreSQL.

Baixa, extrai, normaliza e carrega ~214 milhões de registros via `COPY FROM STDIN` com upsert idempotente, suportando reuso de artefatos entre execuções.

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
# Conexão PostgreSQL
DB_SERVER=seu-host
DB_DATABASE=sandbox
DB_USERNAME=seu-usuario
DB_PASSWORD=sua-senha

# Alternativa: URL completa (tem prioridade sobre as variáveis acima)
# DATABASE_URL=postgresql://usuario:senha@host:5432/banco

# Comportamento
LOG_LEVEL=INFO           # DEBUG | INFO | WARNING | ERROR
FORCE_SYNC=false         # true → reprocessa mesmo snapshot já carregado
REUSE_PROCESSED=true     # true → reutiliza data/processed/ entre execuções
SNAPSHOT_DATE=           # vazio → snapshot mais recente; ex: 2026-04 ou 2026-04-01
```

## Execução

```bash
# Sincronização completa com o snapshot mais recente
python main.py

# Forçar redownload e reprocessamento
set FORCE_SYNC=true && python main.py        # Windows
FORCE_SYNC=true python main.py              # Linux/macOS

# Snapshot específico
set SNAPSHOT_DATE=2026-04 && python main.py

# Nível de log detalhado
set LOG_LEVEL=DEBUG && python main.py
```

## Estrutura do Projeto

```
cnpj-sync-service/
├── main.py                  # Entrypoint — CNPJSyncApplication
├── requirements.txt
├── .env.example
├── sql/
│   └── schema.sql           # DDL idempotente (PostgreSQL 14+)
├── src/
│   ├── config.py            # Todas as constantes configuráveis
│   ├── crawler.py           # Descoberta de snapshot (WebDAV / HTML)
│   ├── database.py          # Operações PostgreSQL (COPY, upserts, controle)
│   ├── downloader.py        # Download paralelo com resume e retry
│   ├── extractor.py         # Extração de ZIPs
│   ├── logger_enhanced.py   # Logger estruturado (Loguru)
│   ├── models.py            # Modelos Pydantic v2 do pipeline
│   ├── pipeline.py          # Orquestração download → extract → process
│   ├── processor.py         # Normalização de CSVs → parquet/csv processado
│   ├── storage.py           # Writers plugáveis (CSV ou Parquet)
│   └── sync.py              # CNPJSync — coordena pipeline + carga no banco
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

Schema criado a partir de `sql/schema.sql` (idempotente). Tabelas no schema `cnpj`:

| Tabela | Tipo | Linhas (aprox.) |
|---|---|---|
| `empresas` | principal | ~60 M |
| `estabelecimentos` | principal | ~62 M |
| `socios` | principal | ~25 M |
| `simples` | principal | ~40 M |
| `cnaes` / `motivos` / `municipios` / `naturezas` / `paises` / `qualificacoes` | referência | < 10 K cada |
| `controle_sincronizacao` | controle | 1 por execução |
| `controle_arquivos` | controle | 1 por arquivo |

Veja [`docs/database.md`](docs/database.md) para o schema completo.

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
