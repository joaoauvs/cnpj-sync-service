# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run full sync (latest snapshot)
python main.py

# Common flags
python main.py --force                        # re-sync even if already processed
python main.py --date 2026-03-16              # target specific snapshot
python main.py --log-level DEBUG --workers 8  # tune verbosity and parallelism
python main.py --server HOST --database DB --username USER --password PASS
```

There is no test suite and no linter configuration in the repo.

## Architecture

The service downloads public CNPJ data from Receita Federal, transforms it, and bulk-loads it into SQL Server. The entrypoint is `main.py`; all logic lives in `src/`.

### End-to-end flow

```
main.py
  ├── CNPJDatabase (pyodbc connection to SQL Server)
  ├── CNPJSync.check_snapshot_needs_sync()   ← idempotency gate via controle_sincronizacao
  └── CNPJSync.sync_snapshot()
        ├── run_pipeline()                   ← download → extract → process
        │     ├── Reference files (sequential): download_file → extract_zip → process_csv
        │     └── Main files (stage-parallel):
        │           download_all (12 workers) → extract_all (4) → process_all (4)
        ├── _dispatch_group() per file       ← bulk MERGE/INSERT into SQL Server
        ├── update_sync_session()            ← mark SUCESSO/FALHA
        └── _cleanup_temp_files()            ← removes data/ on success
```

### Module responsibilities

| Module | Role |
|---|---|
| `crawler.py` | Discovers latest snapshot via WebDAV PROPFIND on Receita Federal; HTML fallback on `casadosdados.com.br` mirror |
| `downloader.py` | Parallel HTTP download with resume (`Range` header), per-thread sessions, exponential back-off via tenacity |
| `extractor.py` | Unzips to `data/extracted/`; validates with `zipfile.testzip()` |
| `processor.py` | Reads raw RF CSVs (latin-1, `;` separator, no headers), applies `SCHEMAS`, normalises dates/decimals, writes to `data/processed/` |
| `storage.py` | Pluggable writer: `CsvWriter` or `ParquetWriter` (controlled by `STORAGE_BACKEND` in config) |
| `sync.py` | `CNPJSync` class orchestrates pipeline → SQL Server load; handles chunked reads with `CSV_CHUNK_ROWS` |
| `database.py` | All pyodbc SQL Server operations: schema init, `bulk_upsert_*` methods, `controle_sincronizacao` tracking |
| `pipeline.py` | `run_pipeline()` — wires crawler + downloader + extractor + processor into staged parallel execution |
| `models.py` | Pydantic v2 models: `RemoteFile`, `Snapshot`, `DownloadResult`, `ExtractionResult`, `ProcessingResult`, `PipelineRun` |
| `config.py` | All tuneable constants (workers, chunk sizes, URLs, schemas, column definitions) |
| `logger_enhanced.py` | Loguru-based structured logger with correlation IDs; writes per-operation log files under `logs/` |

### Data model

`SCHEMAS` in `config.py` defines column lists for every group (files have no headers). Groups: `Empresas`, `Estabelecimentos`, `Socios`, `Simples` (partitioned, multi-file) + 6 small reference tables (`Cnaes`, `Motivos`, `Municipios`, `Naturezas`, `Paises`, `Qualificacoes`).

SQL schema is in `sql/schema.sql` (idempotent). Two control tables track runs: `cnpj.controle_sincronizacao` (per snapshot) and `cnpj.controle_arquivos` (per file).

### Key configuration knobs (`src/config.py`)

```python
DOWNLOAD_WORKERS = 12      # HTTP threads (network-bound)
EXTRACT_WORKERS  = 4       # ZIP extraction (disk-bound)
PROCESS_WORKERS  = 4       # CSV normalisation (CPU-bound)
DATABASE_WORKERS = 2       # SQL Server bulk load
CSV_CHUNK_ROWS   = 200_000 # pandas chunk size (tune for memory)
STORAGE_BACKEND  = "parquet"  # "csv" or "parquet"
```

### `.env` variables

```
DB_SERVER    SQL Server host
DB_DATABASE  Database name
DB_USERNAME  Login
DB_PASSWORD  Password
```
