# CNPJ Sync Service

Serviço de sincronização dos dados públicos de CNPJ da Receita Federal com um banco SQL Server. Descobre automaticamente o snapshot mais recente disponível, faz o download dos arquivos ZIP, extrai, processa e carrega os dados via bulk MERGE/INSERT, garantindo idempotência em todas as execuções.

## Funcionalidades

- Descoberta automática do snapshot mais recente na Receita Federal (com fallback para mirror)
- Download paralelo dos arquivos ZIP com retentativas e resume por HTTP Range
- Extração e normalização dos CSVs (datas, decimais, encoding ISO-8859-1)
- Carga incremental no SQL Server via bulk MERGE (upsert) — sem reprocessar snapshots já carregados
- Controle de execução persistido em `cnpj.controle_sincronizacao`
- Limpeza automática dos arquivos temporários após carga bem-sucedida
- Relatório de execução em JSON salvo na pasta `logs/`

## Tabelas carregadas

| Tabela                  | Descrição                         | Volume estimado |
| ----------------------- | --------------------------------- | --------------- |
| `cnpj.empresas`         | Dados cadastrais da empresa       | ~60 M linhas    |
| `cnpj.estabelecimentos` | Estabelecimentos (matriz/filiais) | ~60 M linhas    |
| `cnpj.socios`           | Quadro societário                 | ~25 M linhas    |
| `cnpj.simples`          | Optantes pelo Simples Nacional    | ~20 M linhas    |
| `cnpj.cnaes`            | Tabela de CNAEs                   | ~100 linhas     |
| `cnpj.motivos`          | Motivos de situação cadastral     | ~60 linhas      |
| `cnpj.municipios`       | Municípios                        | ~5.500 linhas   |
| `cnpj.naturezas`        | Naturezas jurídicas               | ~100 linhas     |
| `cnpj.paises`           | Países                            | ~260 linhas     |
| `cnpj.qualificacoes`    | Qualificações de sócios           | ~70 linhas      |

## Pré-requisitos

- Python 3.11+
- SQL Server 2019+ com ODBC Driver 18 instalado
- Acesso à internet para o site da Receita Federal

## Instalação

```bash
pip install -r requirements.txt
```

## Configuração

Crie um arquivo `.env` na raiz do projeto:

```env
DB_SERVER=seu-servidor
DB_DATABASE=receita-federal
DB_USERNAME=seu-usuario
DB_PASSWORD=sua-senha
```

As variáveis também podem ser passadas por linha de comando (veja abaixo).

## Uso

```bash
# Sincronizar o snapshot mais recente
python main.py

# Forçar re-sincronização mesmo que o snapshot já tenha sido processado
python main.py --force

# Sincronizar uma data específica
python main.py --date 2026-03-16

# Ajustar nível de log e número de workers
python main.py --log-level DEBUG --workers 8

# Especificar credenciais diretamente
python main.py --server 172.0.0.1 --database receita-federal --username sa --password senha
```

### Argumentos disponíveis

| Argumento           | Padrão         | Descrição                                            |
| ------------------- | -------------- | ---------------------------------------------------- |
| `--force`           | —              | Força sincronização mesmo que snapshot já processado |
| `--date YYYY-MM-DD` | Mais recente   | Data específica do snapshot                          |
| `--log-level`       | `INFO`         | Nível de log: `DEBUG`, `INFO`, `WARNING`, `ERROR`    |
| `--server`          | `$DB_SERVER`   | Endereço do SQL Server                               |
| `--database`        | `$DB_DATABASE` | Nome do banco de dados                               |
| `--username`        | `$DB_USERNAME` | Usuário SQL Server                                   |
| `--password`        | `$DB_PASSWORD` | Senha SQL Server                                     |
| `--workers`         | `4`            | Número de workers para download e processamento      |

## Docker

```bash
# Build
docker build -t cnpj-sync-service .

# Execução
docker run --rm \
  -e DB_SERVER=172.0.0.1 \
  -e DB_DATABASE=receita-federal \
  -e DB_USERNAME=sa \
  -e DB_PASSWORD=SuaSenha \
  -v /mnt/data:/app/data \
  cnpj-sync-service
```

O volume `/app/data` é usado para os arquivos temporários (downloads, CSVs extraídos e processados). Os arquivos são removidos automaticamente após a carga.

## Estrutura do projeto

```
cnpj-sync-service/
├── main.py                 # Entrypoint principal
├── requirements.txt
├── Dockerfile
├── sql/
│   └── schema_prod.sql     # Schema SQL Server (idempotente)
├── src/
│   ├── config.py           # Constantes e configurações centrais
│   ├── crawler.py          # Descoberta de snapshots (WebDAV + HTML fallback)
│   ├── database.py         # Conexão e operações SQL Server
│   ├── downloader.py       # Download paralelo com resume
│   ├── extractor.py        # Extração dos ZIPs
│   ├── logger.py           # Configuração do Loguru
│   ├── models.py           # Modelos Pydantic do pipeline
│   ├── pipeline.py         # Orquestração download → extração → processamento
│   ├── processor.py        # Normalização dos CSVs brutos
│   ├── storage.py          # Writer CSV
│   └── sync.py             # Sincronização com o SQL Server (carga + controle)
├── data/                   # Gerado em runtime (gitignore recomendado)
│   ├── downloads/
│   ├── extracted/
│   └── processed/
└── logs/                   # Relatórios JSON por execução
```

## Fluxo de execução

```
main.py
  └─ Descobre data do snapshot mais recente (Receita Federal)
  └─ Verifica controle_sincronizacao (já processado?)
  └─ CNPJSync.sync_snapshot()
       ├─ run_pipeline()
       │    ├─ Tabelas de referência (sequencial)
       │    │    download → extract → process CSV
       │    └─ Tabelas principais (paralelo, N workers)
       │         download → extract → process CSV
       ├─ Carrega cada CSV no SQL Server (bulk MERGE)
       ├─ Atualiza controle_sincronizacao
       └─ Remove arquivos temporários (data/)
```

## Schema do banco

O script `sql/schema_prod.sql` é idempotente (`IF NOT EXISTS`) e cria:

- Schema `cnpj`
- Todas as tabelas com índices otimizados para leitura e carga
- Tabela `cnpj.controle_sincronizacao` para controle de execuções
- Tabela `cnpj.controle_arquivos` para rastreamento por arquivo

Execute manualmente ou deixe o serviço inicializar automaticamente na primeira execução.

## Fontes de dados

- **Primária:** [Receita Federal — Dados Abertos CNPJ](https://arquivos.receitafederal.gov.br)
- **Fallback:** [dados-abertos-rf-cnpj.casadosdados.com.br](https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/)
