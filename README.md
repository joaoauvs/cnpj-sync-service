# CNPJ Sync Service

Serviço de sincronização dos dados públicos de CNPJ da Receita Federal para SQL Server.

O fluxo atual é:

1. Descobrir o snapshot mais recente.
2. Reusar arquivos já baixados em `data/downloads` quando válidos.
3. Reusar artefatos já processados em `data/processed` quando disponíveis.
4. Extrair e normalizar os arquivos restantes.
5. Carregar no SQL Server com `BCP + staging + MERGE/INSERT` quando `bcp.exe` estiver disponível.

## Visão Geral

- Descoberta automática via WebDAV da Receita Federal com fallback HTML.
- Download paralelo com retentativa, resume por `Range` e validação local de ZIP.
- Processamento em chunks para CSV e Parquet.
- Carga incremental e idempotente no SQL Server.
- Controle de execução em `cnpj.controle_sincronizacao` e `cnpj.controle_arquivos`.
- Logs estruturados e relatório JSON por execução.
- Arquitetura orientada a objeto no fluxo principal.

## Arquitetura

O projeto foi reorganizado para concentrar responsabilidades em classes de serviço:

- [main.py](D:/cnpj-sync-service/main.py): `CNPJSyncApplication`
- [src/crawler.py](D:/cnpj-sync-service/src/crawler.py): `SnapshotCrawler`
- [src/downloader.py](D:/cnpj-sync-service/src/downloader.py): `FileDownloader`
- [src/extractor.py](D:/cnpj-sync-service/src/extractor.py): `ZipExtractor`
- [src/processor.py](D:/cnpj-sync-service/src/processor.py): `CSVProcessor`
- [src/pipeline.py](D:/cnpj-sync-service/src/pipeline.py): `CNPJPipeline`
- [src/sync.py](D:/cnpj-sync-service/src/sync.py): `CNPJSync`, `ProcessedFileReader`, `DataFrameNormalizer`
- [src/database.py](D:/cnpj-sync-service/src/database.py): `SQLServerConnection`, `CNPJDatabase`

As funções públicas dos módulos foram mantidas como fachadas finas para compatibilidade.

## Carga no Banco

O carregamento prioriza `bcp.exe` quando disponível.

- `DB_LOAD_ENGINE=auto`: usa BCP quando encontrado no PATH, senão cai para `pyodbc`
- `DB_LOAD_ENGINE=bcp`: exige `bcp.exe`
- `DB_LOAD_ENGINE=pyodbc`: força o caminho antigo

Hoje o caminho BCP cobre:

- `Empresas`
- `Estabelecimentos`
- `Socios`
- `Simples`
- tabelas de referência (`Cnaes`, `Motivos`, `Municipios`, `Naturezas`, `Paises`, `Qualificacoes`)

## Reuso de Artefatos

Para reduzir tempo quando o banco foi limpo mas os arquivos locais já existem:

- ZIPs válidos em `data/downloads` não são baixados novamente
- artefatos em `data/processed` podem ser reutilizados

Variáveis relevantes:

- `FORCE_SYNC=true`: força redownload/reprocessamento
- `REUSE_PROCESSED=true`: reutiliza `data/processed` quando possível

Padrão atual:

- `REUSE_PROCESSED=true`
- se `FORCE_SYNC=true`, o reuse é desativado

## Pré-requisitos

- Python 3.11+
- SQL Server acessível por ODBC
- Microsoft ODBC Driver 18 for SQL Server
- `bcp.exe` no PATH para melhor desempenho

## Instalação

```bash
pip install -r requirements.txt
```

## Configuração

Crie um arquivo `.env` na raiz:

```env
DB_SERVER=seu-servidor
DB_DATABASE=receita-federal
DB_USERNAME=seu-usuario
DB_PASSWORD=sua-senha

LOG_LEVEL=INFO
FORCE_SYNC=false
REUSE_PROCESSED=true
SNAPSHOT_DATE=
DB_LOAD_ENGINE=auto
```

## Execução

Execução padrão:

```bash
python main.py
```

Forçando reload completo:

```bash
set FORCE_SYNC=true
python main.py
```

Forçando engine BCP:

```bash
set DB_LOAD_ENGINE=bcp
python main.py
```

Forçando fallback `pyodbc`:

```bash
set DB_LOAD_ENGINE=pyodbc
python main.py
```

Sincronizando um snapshot específico:

```bash
set SNAPSHOT_DATE=2026-03
python main.py
```

ou:

```bash
set SNAPSHOT_DATE=2026-03-16
python main.py
```

## Principais Configurações

Em [src/config.py](D:/cnpj-sync-service/src/config.py):

```python
DOWNLOAD_WORKERS = 12
EXTRACT_WORKERS = 4
PROCESS_WORKERS = 4
DATABASE_WORKERS = 2
CSV_CHUNK_ROWS = 200_000
STORAGE_BACKEND = "parquet"
```

## Estrutura do Projeto

```text
cnpj-sync-service/
├── main.py
├── requirements.txt
├── sql/
│   └── schema.sql
├── src/
│   ├── config.py
│   ├── crawler.py
│   ├── database.py
│   ├── downloader.py
│   ├── extractor.py
│   ├── logger_enhanced.py
│   ├── models.py
│   ├── pipeline.py
│   ├── processor.py
│   ├── storage.py
│   └── sync.py
├── data/
│   ├── downloads/
│   ├── extracted/
│   └── processed/
└── logs/
```

## Fluxo de Execução

```text
CNPJSyncApplication
  -> CNPJSync
     -> SnapshotCrawler
     -> CNPJPipeline
        -> FileDownloader
        -> ZipExtractor
        -> CSVProcessor
     -> CNPJDatabase
```

Resumo do pipeline:

- referência: download -> extract -> process -> load
- grupos grandes: download paralelo -> extract paralelo -> process paralelo -> load em chunks

## Banco de Dados

O schema é criado a partir de [sql/schema.sql](D:/cnpj-sync-service/sql/schema.sql).

Tabelas principais:

- `cnpj.empresas`
- `cnpj.estabelecimentos`
- `cnpj.socios`
- `cnpj.simples`
- `cnpj.cnaes`
- `cnpj.motivos`
- `cnpj.municipios`
- `cnpj.naturezas`
- `cnpj.paises`
- `cnpj.qualificacoes`
- `cnpj.controle_sincronizacao`
- `cnpj.controle_arquivos`

## Logs e Observabilidade

O projeto grava logs estruturados em `logs/` e também gera um relatório JSON por execução.

Exemplos de informação coletada:

- duração por etapa
- bytes baixados
- linhas processadas
- linhas inválidas
- arquivos com falha
- resumo final da execução

## Fontes de Dados

- [Receita Federal](https://arquivos.receitafederal.gov.br)
- [Casa dos Dados](https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/)

## Observações

- O projeto hoje é configurado principalmente por variáveis de ambiente, não por argumentos CLI.
- Não há suíte de testes no repositório.
- `data/processed` pode ser preservado para acelerar recargas quando só o banco foi resetado.
