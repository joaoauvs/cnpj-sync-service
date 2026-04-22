# AGENTS.md

Guia para agentes de IA (Claude Code, Codex, Copilot) que trabalham neste repositório.

## Comandos Essenciais

```bash
# Instalar dependências
pip install -r requirements.txt

# Executar sincronização completa
python main.py

# Sincronizar snapshot específico
set SNAPSHOT_DATE=2026-04 && python main.py       # Windows
SNAPSHOT_DATE=2026-04 python main.py              # Linux/macOS

# Forçar reprocessamento completo
set FORCE_SYNC=true && python main.py

# Reutilizar artefatos processados (apenas recarregar no banco)
set REUSE_PROCESSED=true && python main.py

# Log detalhado
set LOG_LEVEL=DEBUG && python main.py
```

> **Atenção**: o projeto usa variáveis de ambiente, não flags CLI. Não existe `--force`, `--date` ou `--workers`.

Não há suite de testes nem configuração de linter no repositório.

## Arquitetura

O serviço baixa dados públicos de CNPJ da Receita Federal, normaliza e carrega em bulk no PostgreSQL. Entrypoint: `main.py`. Toda a lógica está em `src/`.

### Fluxo End-to-End

```
main.py (CNPJSyncApplication)
  ├── CNPJDatabase              ← conexão psycopg2 ao PostgreSQL
  ├── CNPJSync.check_snapshot_needs_sync()   ← idempotência via controle_sincronizacao
  └── CNPJSync.sync_snapshot()
        ├── CNPJPipeline.run()
        │     ├── Arquivos de referência (sequencial):
        │     │     download → extract → process → load
        │     └── Arquivos principais (paralelo por estágio):
        │           download_all (12 workers)
        │           → extract_all (4 workers)
        │           → process_all (4 workers)
        ├── _dispatch_group() por arquivo    ← COPY + INSERT ON CONFLICT
        ├── update_sync_session()            ← marca SUCESSO ou FALHA
        └── _cleanup_temp_files()            ← limpa data/ ao final
```

### Responsabilidades por Módulo

| Módulo | Classe principal | Papel |
|---|---|---|
| `main.py` | `CNPJSyncApplication` | Entrypoint, leitura de env vars, ciclo de vida |
| `src/crawler.py` | `SnapshotCrawler` | Descoberta de snapshot via WebDAV PROPFIND; fallback HTML |
| `src/downloader.py` | `FileDownloader` | Download paralelo com resume (`Range`), retry exponencial (tenacity) |
| `src/extractor.py` | `ZipExtractor` | Extração de ZIPs; valida com `zipfile.testzip()` |
| `src/processor.py` | `CSVProcessor` | Lê CSVs latin-1 com `;`, aplica `SCHEMAS`, normaliza datas/decimais, grava em `data/processed/` |
| `src/storage.py` | `CsvWriter`, `ParquetWriter` | Writers plugáveis controlados por `STORAGE_BACKEND` |
| `src/pipeline.py` | `CNPJPipeline` | Orquestra crawler + downloader + extractor + processor em estágios paralelos |
| `src/sync.py` | `CNPJSync`, `ProcessedFileReader`, `DataFrameNormalizer` | Lê artefatos processados e dispara carga no banco por grupo |
| `src/database.py` | `CNPJDatabase` | Todas as operações PostgreSQL: schema, `bulk_upsert_*` via COPY FROM STDIN, controle de sessão |
| `src/models.py` | Pydantic v2 | `RemoteFile`, `Snapshot`, `DownloadResult`, `ExtractionResult`, `ProcessingResult`, `PipelineRun` |
| `src/config.py` | — | Todas as constantes (workers, chunks, URLs, schemas de colunas) |
| `src/logger_enhanced.py` | — | Logger Loguru com IDs de correlação; grava em `logs/` |

### Modelo de Dados

`SCHEMAS` em `config.py` define as colunas de cada grupo (os CSVs da RF não têm cabeçalho).

Grupos particionados (múltiplos arquivos): `Empresas`, `Estabelecimentos`, `Socios`  
Arquivos de referência (arquivo único): `Cnaes`, `Motivos`, `Municipios`, `Naturezas`, `Paises`, `Qualificacoes`  
Grupo especial (arquivo único): `Simples`

### Carga no Banco

Todos os grupos passam pelo mesmo padrão:

1. `ProcessedFileReader` lê parquet/CSV em chunks
2. Função de preparação converte tipos, trata nulos e trunca strings ao limite da coluna
3. `_df_to_copy_buffer()` serializa para formato `COPY TEXT` (tab-delimited, `\N` para NULL)
4. `COPY tmp_* FROM STDIN` carrega na tabela temporária (all-TEXT)
5. `INSERT … ON CONFLICT … DO UPDATE` faz upsert tipado na tabela final

### Tratamento de Nulos

A constante modular `_NULL_STRS` em `database.py` define todas as strings que representam valores nulos (`"nan"`, `"NaN"`, `"None"`, `"NULL"`, `"<NA>"`, `"na"`, `"NA"`, `"N/A"`, etc.). Três camadas de proteção:

1. Lambda `apply` em cada função de preparação checa `v in _NULL_STRS`
2. `staged.replace(list(_NULL_STRS), None)` ao final de cada preparação
3. `_df_to_copy_buffer` detecta `None`, `float NaN`, string vazia e `_NULL_STRS` → emite `\N`

### Knobs de Configuração (`src/config.py`)

```python
DOWNLOAD_WORKERS = 12      # threads HTTP (network-bound)
EXTRACT_WORKERS  = 4       # threads de extração (disk-bound)
PROCESS_WORKERS  = 4       # threads de normalização CSV (CPU-bound)
CSV_CHUNK_ROWS   = 200_000 # chunk pandas (controle de memória)
STORAGE_BACKEND  = "parquet"  # "csv" ou "parquet"
REQUEST_TIMEOUT  = 120     # timeout HTTP em segundos
MAX_RETRIES      = 5       # tentativas de download
```

### Variáveis de Ambiente

| Variável | Padrão | Descrição |
|---|---|---|
| `DB_SERVER` | `72.60.4.227` | Host PostgreSQL |
| `DB_DATABASE` | `sandbox` | Nome do banco |
| `DB_USERNAME` | — | Usuário (ou `POSTGRES_USER`) |
| `DB_PASSWORD` | — | Senha (ou `POSTGRES_PASSWORD`) |
| `DATABASE_URL` | — | URL completa — tem prioridade sobre as individuais |
| `LOG_LEVEL` | `INFO` | `DEBUG \| INFO \| WARNING \| ERROR` |
| `FORCE_SYNC` | `false` | `true` → ignora idempotência e reprocessa |
| `REUSE_PROCESSED` | `true` | `true` → reutiliza `data/processed/` |
| `SNAPSHOT_DATE` | vazio | `YYYY-MM` ou `YYYY-MM-DD`; vazio = mais recente |

## Convenções Importantes

- **Sem CLI flags**: toda configuração via variáveis de ambiente ou `.env`
- **Idempotência**: a tabela `cnpj.controle_sincronizacao` impede dupla carga do mesmo snapshot; use `FORCE_SYNC=true` para forçar
- **Reuse**: com `REUSE_PROCESSED=true`, só o banco é recarregado se os arquivos em `data/processed/` já existirem
- **Encoding**: CSVs da RF são latin-1 com separador `;` sem cabeçalho
- **Null bytes**: strings vindas dos CSVs podem conter `\x00` — são removidos em `_df_to_copy_buffer`
- **Schema**: criado via `sql/schema.sql` (idempotente, `CREATE TABLE IF NOT EXISTS`)
