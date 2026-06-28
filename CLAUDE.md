# CLAUDE.md

## Project

**cnpj-sync-service** — high-performance Python data pipeline that downloads, normalizes, and loads ~196 million Brazilian CNPJ records from Receita Federal into PostgreSQL. Idempotent: tracks progress via control tables so interrupted runs resume without re-downloading.

## Structure

```
src/
├── config.py           # All tuneable constants (workers, paths, URLs)
├── crawler.py          # Snapshot discovery (WebDAV + HTML fallback)
├── downloader.py       # Parallel download with resume & retry
├── extractor.py        # ZIP extraction
├── processor.py        # CSV normalization (dates, decimals, encoding)
├── storage.py          # Pluggable writers: CSV or Parquet
├── database.py         # PostgreSQL COPY + upsert + control tables
├── pipeline.py         # Orchestrates download → extract → process
├── sync.py             # CNPJSync top-level coordinator
├── models.py           # Pydantic v2 models
└── logger_enhanced.py  # Loguru structured logging
sql/
└── schema.sql          # Idempotent DDL — 8 tables + vw_empresas_completo view
data/
├── downloads/          # ZIPs (reused between runs — do not delete)
├── extracted/          # Temporary CSVs (safe to delete)
└── processed/          # Normalized Parquet/CSV artifacts
```

## Commands

```bash
python main.py --help                         # All flags
python main.py                                # Full sync (latest snapshot)
python main.py --workers-download 12 --workers-extract 4 --workers-process 4
python main.py --date 2024-01-01             # Sync a specific snapshot date
pytest                                        # Run test suite
pytest tests/test_normalizer.py              # Run a single module
```

## Conventions

- **`src/config.py` is the single source of truth** for parallelism, paths, and URLs — do not hardcode these elsewhere.
- **Resume-safe:** `cnpj.controle_sincronizacao` and `cnpj.controle_arquivos` track which files have been processed. `database.py` upserts, never truncates + reinserts.
- **`schema.sql` is idempotent** (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`). Apply it to a fresh DB before first run. Apply it again after schema changes.
- **Pydantic v2 models** in `models.py` for data crossing module boundaries. Use `model_validator` over `__init__` logic.
- **`loguru`** for all logging — do not use `print()` or `logging` directly.
- **Parallelism levers:** download (default 12), extraction (4), processing (4). Tune via CLI flags, not config file edits.
- **Tests:** unit tests in `tests/unit/`, integration tests (require a live DB) in `tests/integration/`. Mark integration tests with `@pytest.mark.integration`.

## Environment

Copy `.env.example` to `.env`:

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | PostgreSQL DSN (`postgresql://user:pass@host:5432/db`) |
| `DATA_DIR` | Override default `data/` directory (optional) |
| `LOG_LEVEL` | `DEBUG`, `INFO`, `WARNING` (default: `INFO`) |

## Schema notes

Four main tables: `cnpj.empresas`, `cnpj.estabelecimentos`, `cnpj.socios`, `cnpj.simples_nacional`. View `cnpj.vw_empresas_completo` joins them with formatted CNPJ. Any schema change requires a new migration script applied manually in SQL; update `schema.sql` accordingly.
