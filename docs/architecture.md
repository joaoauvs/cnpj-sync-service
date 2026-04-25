# Arquitetura — CNPJ Sync Service

## Visão Geral

O serviço é composto por um pipeline de cinco estágios executados em sequência, com paralelismo dentro de cada estágio para os arquivos principais.

```
Descoberta → Download → Extração → Processamento → Carga
```

## Classes Principais

```
CNPJSyncApplication (main.py)
│
└── CNPJSync (src/sync.py)
      ├── SnapshotCrawler   (src/crawler.py)
      ├── CNPJPipeline      (src/pipeline.py)
      │     ├── FileDownloader  (src/downloader.py)
      │     ├── ZipExtractor    (src/extractor.py)
      │     └── CSVProcessor    (src/processor.py)
      │           └── Storage   (src/storage.py)
      └── CNPJDatabase      (src/database.py)
```

## Estágios do Pipeline

### 1. Descoberta (`SnapshotCrawler`)

- Faz `PROPFIND` no WebDAV da Receita Federal para listar snapshots disponíveis
- Seleciona o mais recente ou o especificado por `SNAPSHOT_DATE`
- Em caso de falha WebDAV, faz fallback para scraping HTML do mirror Casa dos Dados
- Retorna um objeto `Snapshot` com a lista de `RemoteFile`

### 2. Download (`FileDownloader`)

- Pool de 12 threads (padrão) via `ThreadPoolExecutor`
- Resume por `Range: bytes=N-` quando o ZIP local está incompleto
- Retry exponencial via tenacity (5 tentativas, fator 2.0)
- Valida integridade do ZIP após download
- Pula arquivos cujo ZIP local já é válido (idempotente)

### 3. Extração (`ZipExtractor`)

- Pool de 4 threads
- Extrai para `data/extracted/<zip_stem>/`
- Valida com `zipfile.testzip()` antes de extrair

### 4. Processamento (`CSVProcessor`)

- Pool de 4 threads
- Lê cada CSV em chunks de 200.000 linhas com `pandas.read_csv`
  - Encoding: `latin-1` (ISO-8859-1)
  - Separador: `;`
  - Sem cabeçalho — colunas definidas por `SCHEMAS` em `config.py`
  - `dtype=str`, `na_values=[""]`, `keep_default_na=False`
- Normaliza datas (`YYYYMMDD` → `YYYY-MM-DD`)
- Normaliza decimais (`1234,56` → `1234.56`) — RF usa vírgula como separador decimal sem separador de milhar
- Grava em `data/processed/` como Parquet ou CSV

### 5. Carga (`CNPJDatabase`)

- `ProcessedFileReader` lê os artefatos processados em chunks
- Para cada chunk, a função de preparação do grupo:
  1. Filtra linhas inválidas (cnpj_basico nulo)
  2. Converte tipos e trata nulos (`_NULL_STRS`)
  3. Trunca strings ao limite da coluna VARCHAR
- `_df_to_copy_buffer()` serializa para formato COPY TEXT
- `COPY tmp_* FROM STDIN` carrega na tabela temporária (tudo TEXT)
- `INSERT … ON CONFLICT … DO UPDATE` faz upsert tipado
- Cada chunk é comitado individualmente

## Paralelismo

| Estágio | Workers | Tipo de limite |
|---|---|---|
| Download | 12 | I/O de rede |
| Extração | 4 | I/O de disco |
| Processamento | 4 | CPU |
| Carga | 1 por grupo | I/O de banco |

Os três primeiros estágios são executados em paralelo por arquivo dentro de cada estágio. A carga é sequencial por arquivo mas paralela entre grupos independentes.

## Idempotência

- `cnpj.controle_sincronizacao` registra cada execução por snapshot
- Um índice único parcial em `status = 'EM_EXECUCAO'` impede execuções simultâneas do mesmo snapshot
- `INSERT … ON CONFLICT … DO UPDATE` garante que reexecuções atualizem em vez de duplicar
- Para socios: `DELETE` + `INSERT` (sem chave natural única)
- `FORCE_SYNC=true` ignora a verificação de snapshot já processado

## Reuso de Artefatos

```
REUSE_PROCESSED=true (padrão)
  ├── ZIPs em data/downloads/  → validados e reutilizados se íntegros
  └── Parquets em data/processed/ → reutilizados sem reprocessamento

FORCE_SYNC=true
  └── Ignora tudo e reprocessa do zero
```

## Tratamento de Falhas

- Falha em um arquivo não interrompe os demais — o erro é registrado em `cnpj.controle_arquivos`
- Cada chunk de carga tem seu próprio `try/except` com `rollback` em caso de erro
- A sessão é marcada como `FALHA` se qualquer arquivo falhar
- Logs estruturados em `logs/` com timestamp e contexto da operação
