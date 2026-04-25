# Banco de Dados — CNPJ Sync Service

## Conexão

O serviço conecta via `psycopg2` ao PostgreSQL. A conexão é configurada por variáveis de ambiente:

```env
DATABASE_URL=postgresql://usuario:senha@host:5432/banco   # tem prioridade
# ou individualmente:
DB_SERVER=host
DB_DATABASE=banco
DB_USERNAME=usuario
DB_PASSWORD=senha
```

A classe `PostgreSQLConnection` em `src/database.py` gerencia o pool de conexões com retry exponencial via tenacity.

## Schema

O schema é criado a partir de `sql/schema.sql` (idempotente — usa `CREATE TABLE IF NOT EXISTS`). Todas as tabelas ficam no schema `cnpj`.

### Tabelas Principais

#### `cnpj.empresas`

```sql
cnpj_basico                 VARCHAR(8)     NOT NULL  -- PK
razao_social                VARCHAR(150)   NOT NULL
natureza_juridica           VARCHAR(4)     NOT NULL
qualificacao_responsavel    VARCHAR(2)     NOT NULL
capital_social              NUMERIC(18,2)  NULL
porte_empresa               VARCHAR(2)     NULL
ente_federativo_responsavel VARCHAR(50)    NULL
snapshot_date               DATE           NOT NULL
data_carga                  TIMESTAMP      NOT NULL DEFAULT NOW()
```

~60 milhões de registros. Upsert por `cnpj_basico`.

#### `cnpj.estabelecimentos`

```sql
cnpj_completo               VARCHAR(14)   NOT NULL  -- PK (basico+ordem+dv)
cnpj_basico                 VARCHAR(8)    NOT NULL
cnpj_ordem                  VARCHAR(4)    NOT NULL
cnpj_dv                     VARCHAR(2)    NOT NULL
identificador_matriz_filial VARCHAR(1)    NULL
nome_fantasia               VARCHAR(55)   NULL
situacao_cadastral          VARCHAR(2)    NULL
data_situacao_cadastral     DATE          NULL
motivo_situacao_cadastral   VARCHAR(2)    NULL
nome_cidade_exterior        VARCHAR(55)   NULL
pais                        VARCHAR(3)    NULL
data_inicio_atividade       DATE          NULL
cnae_fiscal                 VARCHAR(10)   NULL
cnae_fiscal_secundaria      VARCHAR(1000) NULL
tipo_logradouro             VARCHAR(20)   NULL
logradouro                  VARCHAR(60)   NULL
numero                      VARCHAR(6)    NULL
complemento                 VARCHAR(156)  NULL
bairro                      VARCHAR(50)   NULL
cep                         VARCHAR(8)    NULL
uf                          VARCHAR(2)    NULL
municipio                   VARCHAR(4)    NULL
ddd_1 / telefone_1          VARCHAR(4/9)  NULL
ddd_2 / telefone_2          VARCHAR(4/9)  NULL
ddd_fax / fax               VARCHAR(4/9)  NULL
correio_eletronico          VARCHAR(115)  NULL
situacao_especial           VARCHAR(23)   NULL
data_situacao_especial      DATE          NULL
snapshot_date               DATE           NOT NULL
data_carga                  TIMESTAMP      NOT NULL DEFAULT NOW()
```

~71 milhões de registros. Upsert por `cnpj_completo`.

#### `cnpj.socios`

```sql
id_socio                         BIGSERIAL    NOT NULL  -- PK gerada
cnpj_basico                      VARCHAR(8)   NOT NULL
identificador_socio              VARCHAR(1)   NOT NULL
nome_socio_razao_social          VARCHAR(150) NOT NULL
cnpj_cpf_socio                   VARCHAR(14)  NULL
qualificacao_socio               VARCHAR(2)   NOT NULL
data_entrada_sociedade           DATE         NULL
pais                             VARCHAR(3)   NULL
representante_legal              VARCHAR(11)  NULL
nome_representante               VARCHAR(60)  NULL
qualificacao_representante_legal VARCHAR(2)   NULL
faixa_etaria                     VARCHAR(1)   NULL
snapshot_date                    DATE         NOT NULL
data_carga                       TIMESTAMP    NOT NULL DEFAULT NOW()
```

~25 milhões de registros. **Sem chave natural única** — a carga usa `DELETE + INSERT` por lote de `cnpj_basico`.

#### `cnpj.simples`

```sql
cnpj_basico           VARCHAR(8) NOT NULL  -- PK
opcao_pelo_simples    VARCHAR(1) NULL
data_opcao_simples    DATE       NULL
data_exclusao_simples DATE       NULL
opcao_mei             VARCHAR(1) NULL
data_opcao_mei        DATE       NULL
data_exclusao_mei     DATE       NULL
snapshot_date         DATE       NOT NULL
data_carga            TIMESTAMP  NOT NULL DEFAULT NOW()
```

~40 milhões de registros. Upsert por `cnpj_basico`.

### Tabelas de Referência

Todas com estrutura `(codigo VARCHAR, descricao VARCHAR, snapshot_date DATE, data_carga TIMESTAMP)`:

| Tabela | PK | Registros |
|---|---|---|
| `cnpj.cnaes` | `codigo VARCHAR(10)` | ~1.400 |
| `cnpj.motivos` | `codigo VARCHAR(2)` | ~63 |
| `cnpj.municipios` | `codigo VARCHAR(4)` | ~5.600 |
| `cnpj.naturezas` | `codigo VARCHAR(4)` | ~91 |
| `cnpj.paises` | `codigo VARCHAR(3)` | ~255 |
| `cnpj.qualificacoes` | `codigo VARCHAR(2)` | ~68 |

### Tabelas de Controle

#### `cnpj.controle_sincronizacao`

Registra cada execução do serviço. Status possíveis: `EM_EXECUCAO`, `SUCESSO`, `FALHA`, `CANCELADO`.

Um índice único parcial garante que nunca haja dois `EM_EXECUCAO` para o mesmo snapshot.

#### `cnpj.controle_arquivos`

Registra o resultado por arquivo dentro de uma execução. Status: `PENDENTE`, `DOWNLOAD`, `EXTRACAO`, `PROCESSAMENTO`, `SUCESSO`, `FALHA`.

## Estratégia de Carga

### Padrão para tabelas com chave natural (empresas, estabelecimentos, simples, referências)

```sql
-- 1. Tabela temporária all-TEXT (descartada no fim da transação)
CREATE TEMP TABLE tmp_* (...colunas TEXT...) ON COMMIT DROP;

-- 2. COPY FROM STDIN (bulk insert via psycopg2)
COPY tmp_* FROM STDIN;  -- formato: tab-delimited, \N para NULL

-- 3. Upsert tipado
INSERT INTO cnpj.tabela (col1, col2, ...)
SELECT NULLIF(col1, ''), col2::DATE, ...
FROM tmp_*
WHERE NULLIF(cnpj_basico, '') IS NOT NULL
ON CONFLICT (pk) DO UPDATE SET
    col1 = EXCLUDED.col1,
    ...
    data_carga = NOW()
RETURNING (xmax = 0);  -- distingue insert de update
```

### Padrão para socios (sem chave natural)

```sql
-- DELETE em lotes de 1.000 cnpj_basico
DELETE FROM cnpj.socios
WHERE snapshot_date = %s AND cnpj_basico = ANY(%s);

-- INSERT em bloco
INSERT INTO cnpj.socios (...) SELECT ... FROM tmp_socios_*;
```

## View

### `cnpj.vw_empresas_completo`

View desnormalizada com join de todas as tabelas. Projetada para consultas analíticas e exportações.

**Tabelas envolvidas:**

| Tabela | Join | Propósito |
|---|---|---|
| `estabelecimentos` | base | CNPJ completo, endereço, contato, situação |
| `empresas` | `LEFT JOIN cnpj_basico` | Razão social, capital, porte, natureza |
| `naturezas` | `LEFT JOIN natureza_juridica` | Descrição da natureza jurídica |
| `qualificacoes` | `LEFT JOIN qualificacao_responsavel` | Descrição da qualificação do responsável |
| `motivos` | `LEFT JOIN motivo_situacao_cadastral` | Descrição do motivo de situação |
| `cnaes` | `LEFT JOIN cnae_fiscal` | Descrição da atividade econômica principal |
| `municipios` | `LEFT JOIN municipio` | Nome do município |
| `paises` | `LEFT JOIN pais` | Nome do país (estabelecimentos no exterior) |
| `simples` | `LEFT JOIN cnpj_basico` | Opção Simples Nacional / MEI |

**Colunas principais:**

```sql
cnpj_completo          VARCHAR(14)   -- CNPJ sem formatação (14 dígitos)
cnpj_formatado         TEXT          -- XX.XXX.XXX/XXXX-XX
cnpj_basico            VARCHAR(8)
razao_social           VARCHAR(150)
natureza_juridica_descricao  TEXT
porte_empresa_descricao      TEXT    -- 'Micro Empresa', 'EPP', 'Demais', etc.
tipo_unidade                 TEXT    -- 'Matriz' ou 'Filial'
situacao_cadastral_descricao TEXT    -- 'Ativa', 'Baixada', 'Inapta', etc.
cnae_fiscal_descricao        TEXT
municipio_descricao          TEXT
opcao_pelo_simples     VARCHAR(1)
opcao_mei              VARCHAR(1)
```

**Exemplo de uso:**

```sql
-- Empresas ativas em SP com CNAE de tecnologia
SELECT cnpj_formatado, razao_social, municipio_descricao, cnae_fiscal_descricao
FROM cnpj.vw_empresas_completo
WHERE situacao_cadastral = '02'
  AND uf = 'SP'
  AND cnae_fiscal LIKE '62%'
LIMIT 100;

-- Contagem por situação
SELECT situacao_cadastral_descricao, COUNT(*)
FROM cnpj.vw_empresas_completo
GROUP BY situacao_cadastral_descricao
ORDER BY COUNT(*) DESC;
```

> **Atenção:** a view faz join de ~71 M linhas. Para queries de produção, adicione sempre filtros indexados (`cnpj_completo`, `cnpj_basico`, `uf`, `situacao_cadastral`, `cnae_fiscal`, `municipio`).

## Índices

Todos os índices usam `CREATE INDEX IF NOT EXISTS`. Índices principais:

- `ix_empresas_natureza` — `natureza_juridica`
- `ix_empresas_porte` — `porte_empresa INCLUDE (razao_social, cnpj_basico)`
- `ix_estab_cnpj_basico` — `cnpj_basico INCLUDE (situacao_cadastral, cnae_fiscal, uf, municipio)`
- `ix_estab_situacao_uf` — `(situacao_cadastral, uf) INCLUDE (cnpj_basico, cnpj_completo, nome_fantasia)`
- `ix_estab_municipio` — `municipio INCLUDE (cnpj_basico, situacao_cadastral)`
- `ix_estab_cnae` — `cnae_fiscal INCLUDE (cnpj_basico, situacao_cadastral, uf)`
- `ix_socios_cnpj_basico` — `cnpj_basico INCLUDE (nome_socio_razao_social, qualificacao_socio)`
- `ix_simples_simples` / `ix_simples_mei` — filtros por opção
