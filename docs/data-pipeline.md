# Pipeline de Dados — CNPJ Sync Service

## Fonte

Os dados são publicados mensalmente pela Receita Federal no formato de ZIPs contendo CSVs.

- **Encoding**: latin-1 (ISO-8859-1)
- **Separador**: `;`
- **Sem cabeçalho**: colunas definidas em `SCHEMAS` (`src/config.py`)
- **Snapshot**: diretório datado (ex: `2026-04/`)

## Grupos de Arquivos

### Particionados (múltiplos arquivos por snapshot)

| Grupo | Arquivos | Linhas (aprox.) |
|---|---|---|
| `Empresas` | Empresas0.zip … EmpresasN.zip | ~60 M |
| `Estabelecimentos` | Estabelecimentos0.zip … | ~62 M |
| `Socios` | Socios0.zip … | ~25 M |

### Arquivo único

| Grupo | Arquivo | Linhas (aprox.) |
|---|---|---|
| `Simples` | Simples.zip | ~40 M |
| `Cnaes` | Cnaes.zip | ~1.400 |
| `Motivos` | Motivos.zip | ~63 |
| `Municipios` | Municipios.zip | ~5.600 |
| `Naturezas` | Naturezas.zip | ~91 |
| `Paises` | Paises.zip | ~255 |
| `Qualificacoes` | Qualificacoes.zip | ~68 |

## Normalização no Processamento (`CSVProcessor`)

### Datas (`DATE_COLUMNS`)

Colunas no formato `YYYYMMDD` → `YYYY-MM-DD`:

- `Estabelecimentos`: `data_situacao_cadastral`, `data_inicio_atividade`, `data_situacao_especial`
- `Socios`: `data_entrada_sociedade`
- `Simples`: `data_opcao_simples`, `data_exclusao_simples`, `data_opcao_mei`, `data_exclusao_mei`

Valores inválidos ou `00000000` → string vazia → NULL no banco.

### Decimais (`DECIMAL_COLUMNS`)

- `Empresas.capital_social`: vírgula brasileira → ponto (`1.234,56` → `1234.56`) → `float`

### Nulos

- Células vazias no CSV → `NaN` (via `na_values=[""]`)
- `keep_default_na=False` → valores como `"nan"` no CSV são mantidos como string

### Encoding de caracteres

Leitura em `latin-1`. Escrita em `UTF-8` (parquet ou CSV processado). Bytes nulos `\x00` presentes em alguns campos de endereço são removidos no `_df_to_copy_buffer`.

## Artefatos Processados (`data/processed/`)

Cada ZIP gera um arquivo processado:

```
data/processed/
├── Empresas0.parquet
├── Empresas1.parquet
├── Estabelecimentos0.parquet
├── Socios0.parquet
├── Simples.parquet
├── Cnaes.parquet
└── ...
```

Com `STORAGE_BACKEND=csv`, os arquivos são `.csv` UTF-8 com cabeçalho.

## Preparação para Carga (`database.py`)

Cada grupo tem uma função de preparação dedicada que:

1. **Filtra** linhas com `cnpj_basico` nulo ou vazio
2. **Converte tipos**: datas para string ISO, decimais para float
3. **Trata nulos** (três camadas):
   - Lambda por coluna checa `_NULL_STRS` (`"nan"`, `"NaN"`, `"<NA>"`, etc.)
   - `staged.replace(list(_NULL_STRS), None)` varre todo o DataFrame
   - `_df_to_copy_buffer` captura `None`, `float NaN` e strings nulas → `\N`
4. **Trunca** strings ao limite VARCHAR da coluna de destino
5. **Remove duplicatas** em `cnpj_basico` (mantém a última ocorrência)

## Serialização COPY (`_df_to_copy_buffer`)

Formato TEXT do PostgreSQL (tab-delimited):

```
campo1\tcampo2\t\N\tcampo4\n
```

Regras de escape por tipo de valor:

| Valor | Saída |
|---|---|
| `None` | `\N` |
| `float NaN` | `\N` |
| string vazia ou em `_NULL_STRS` | `\N` |
| `\x00` | removido |
| `\\` | `\\\\` |
| `\t` | espaço |
| `\n` / `\r` | espaço |
| outros | `str(v)` |

## Fluxo de Dados Completo

```
RF CSV (latin-1, ';', sem header)
  │
  ▼ pd.read_csv(dtype=str, na_values=[""], keep_default_na=False)
  │
  ├─ Normalização de datas     YYYYMMDD → YYYY-MM-DD
  ├─ Normalização de decimais  1.234,56 → 1234.56
  └─ Strip de espaços
  │
  ▼ writer.write(chunk)  [ParquetWriter ou CsvWriter]
  │
data/processed/*.parquet  (UTF-8, tipagem preservada)
  │
  ▼ ProcessedFileReader.read()  (chunks de 200k linhas)
  │
  ├─ Filtro de cnpj_basico inválido
  ├─ Conversão de tipos
  ├─ Tratamento de nulos (_NULL_STRS)
  ├─ Truncamento de strings ao limite VARCHAR
  └─ _df_to_copy_buffer()
  │
  ▼ COPY tmp_* FROM STDIN  (PostgreSQL, formato TEXT)
  │
  ▼ INSERT … ON CONFLICT … DO UPDATE  (upsert tipado)
  │
cnpj.* (PostgreSQL, schema cnpj)
```
