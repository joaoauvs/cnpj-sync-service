# Configuração — CNPJ Sync Service

## Variáveis de Ambiente (`.env`)

| Variável | Padrão | Descrição |
|---|---|---|
| `DATABASE_URL` | — | URL completa PostgreSQL — tem prioridade sobre as variáveis individuais abaixo |
| `DB_SERVER` | `localhost` | Host do PostgreSQL |
| `DB_DATABASE` | `postgres` | Nome do banco de dados |
| `DB_USERNAME` | — | Usuário PostgreSQL (alias: `POSTGRES_USER`) |
| `DB_PASSWORD` | — | Senha PostgreSQL (alias: `POSTGRES_PASSWORD`) |
| `LOG_LEVEL` | `INFO` | Nível de log: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `FORCE_SYNC` | `false` | `true` → ignora idempotência e reprocessa do zero |
| `REUSE_PROCESSED` | `true` | `true` → reutiliza `data/processed/` entre execuções |
| `SNAPSHOT_DATE` | vazio | Data alvo: `YYYY-MM` ou `YYYY-MM-DD`; vazio = mais recente |

### Exemplo de `.env`

```env
DB_SERVER=meu-postgres.exemplo.com
DB_DATABASE=cnpj_producao
DB_USERNAME=cnpj_user
DB_PASSWORD=senha-segura

LOG_LEVEL=INFO
FORCE_SYNC=false
REUSE_PROCESSED=true
SNAPSHOT_DATE=
```

### Usando `DATABASE_URL`

```env
DATABASE_URL=postgresql://cnpj_user:senha-segura@meu-postgres.exemplo.com:5432/cnpj_producao
```

## Constantes em `src/config.py`

### Concorrência

```python
DOWNLOAD_WORKERS = 12   # threads de download (limitado pela rede)
EXTRACT_WORKERS  = 4    # threads de extração (limitado por disco)
PROCESS_WORKERS  = 4    # threads de processamento CSV (limitado por CPU)
DATABASE_WORKERS = 2    # threads de carga no banco (limitado por I/O)
```

### Processamento

```python
CSV_ENCODING    = "latin-1"   # encoding dos arquivos da RF (ISO-8859-1)
CSV_SEPARATOR   = ";"         # separador dos CSVs da RF
CSV_CHUNK_ROWS  = 200_000     # linhas por chunk pandas
STORAGE_BACKEND = "parquet"   # "csv" ou "parquet" para data/processed/
```

### HTTP

```python
REQUEST_TIMEOUT     = 120        # timeout por requisição (segundos)
DOWNLOAD_CHUNK_SIZE = 4_194_304  # 4 MB — tamanho do chunk de streaming
MAX_RETRIES         = 5          # tentativas máximas por arquivo
BACKOFF_FACTOR      = 2.0        # multiplicador exponencial entre tentativas
```

### Fontes Remotas

```python
RF_WEBDAV_BASE  = "https://arquivos.receitafederal.gov.br/public.php/webdav"
FALLBACK_BASE_URL = "https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/"
```

### Diretórios

```python
DATA_DIR       = ROOT_DIR / "data"
DOWNLOADS_DIR  = DATA_DIR / "downloads"   # ZIPs baixados
EXTRACTED_DIR  = DATA_DIR / "extracted"   # CSVs extraídos
PROCESSED_DIR  = DATA_DIR / "processed"   # artefatos normalizados
LOGS_DIR       = ROOT_DIR / "logs"
```

## Combinações Comuns

### Primeira carga (banco vazio, sem artefatos locais)

```env
FORCE_SYNC=false
REUSE_PROCESSED=false
```

### Recarga do banco (artefatos já processados disponíveis)

```env
REUSE_PROCESSED=true
FORCE_SYNC=false
```

### Reprocessamento completo (dados da RF atualizados)

```env
FORCE_SYNC=true
REUSE_PROCESSED=false
```

### Debug de um snapshot específico

```env
SNAPSHOT_DATE=2026-03
LOG_LEVEL=DEBUG
REUSE_PROCESSED=true
```
