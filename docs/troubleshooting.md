# Troubleshooting — CNPJ Sync Service

## Erros Conhecidos

### `value too long for type character varying(N)`

**Causa**: um campo string do CSV tem mais caracteres do que o limite da coluna no banco.

**Solução aplicada**:
- Python: loop de truncamento por coluna com `str(v)[:max_len]` antes do COPY
- SQL: `LEFT(col, N)` no SELECT do INSERT para garantia dupla

**Onde**: `bulk_upsert_empresas` (`database.py`), colunas `qualificacao_responsavel VARCHAR(2)` e `porte_empresa VARCHAR(2)`.

---

### `invalid byte sequence for encoding "UTF8": 0x00`

**Causa**: bytes nulos (`\x00`) presentes em alguns campos de texto da Receita Federal (comum em campos de endereço/complemento).

**Solução aplicada**: `v.replace("\x00", "")` em `_df_to_copy_buffer` antes da serialização.

**Onde**: `estabelecimentos`, campos de endereço (`complemento`, `logradouro`, etc.).

---

### Colunas com valor `"nan"` no banco

**Causa**: pandas converte `NaN` para a string `"nan"` em certas operações (`astype(str)`, `str(v)`). Strings `"nan"` de parquets antigos também podem escapar.

**Solução aplicada** (três camadas em `database.py`):
1. Lambda `apply` por coluna checa `v in _NULL_STRS`
2. `staged.replace(list(_NULL_STRS), None)` ao final de cada preparação
3. `_df_to_copy_buffer` detecta `None`, `float NaN`, string vazia e `_NULL_STRS` → emite `\N`

**`_NULL_STRS`**: `"nan"`, `"NaN"`, `"NAN"`, `"None"`, `"NONE"`, `"none"`, `"NULL"`, `"null"`, `"Null"`, `"<NA>"`, `"na"`, `"NA"`, `"N/A"`, `"n/a"`

---

### Execução falha com `EM_EXECUCAO` mas nenhum processo rodando

**Causa**: execução anterior terminou de forma abrupta sem marcar `FALHA`.

**Solução**:
```sql
UPDATE cnpj.controle_sincronizacao
SET status = 'FALHA', data_fim_execucao = NOW()
WHERE status = 'EM_EXECUCAO';
```

Ou use `FORCE_SYNC=true` para ignorar a verificação.

---

### Download muito lento ou falhando

**Causas possíveis**:
- Rede instável com o servidor da Receita Federal
- Rate limiting

**Ações**:
- Reduzir `DOWNLOAD_WORKERS` em `src/config.py` (ex: `6` em vez de `12`)
- Aumentar `REQUEST_TIMEOUT` e `MAX_RETRIES`
- O resume por `Range` garante que downloads parciais não sejam perdidos

---

### Memória insuficiente durante processamento

**Causa**: chunks muito grandes para a RAM disponível.

**Solução**: reduzir `CSV_CHUNK_ROWS` em `src/config.py`:
```python
CSV_CHUNK_ROWS = 100_000  # padrão: 200_000
```

---

### Erro de conexão com PostgreSQL

**Verificações**:
1. Host/porta acessíveis: `psql -h DB_SERVER -p 5432 -U DB_USERNAME -d DB_DATABASE`
2. Variáveis de ambiente corretas no `.env`
3. Firewall/VPN não bloqueando a porta 5432

---

### `data/processed/` corrompido após falha

**Sintoma**: reexecução com `REUSE_PROCESSED=true` falha com erro de leitura de parquet.

**Solução**:
```bash
# Remover apenas os processados e reprocessar
rmdir /s /q data\processed
python main.py
```

Ou deletar apenas o arquivo específico que corrompeu.

---

### Schema desatualizado no banco

**Causa**: o schema foi alterado mas a tabela já existia (`CREATE TABLE IF NOT EXISTS` não altera tabelas existentes).

**Solução**: aplicar as alterações manualmente via `ALTER TABLE`, ou recriar o schema:
```sql
DROP SCHEMA cnpj CASCADE;
-- então reexecutar o serviço que recria via schema.sql
```

## Logs

Logs ficam em `logs/` com timestamp. Para ver o log em tempo real:

```bash
# Windows PowerShell
Get-Content logs\*.log -Wait

# Linux/macOS
tail -f logs/*.log
```

Aumentar verbosidade:
```env
LOG_LEVEL=DEBUG
```

## Testes automatizados

Para verificar conectividade, estrutura do banco e integridade dos dados:

```bash
pip install -r requirements-dev.txt

pytest                          # suite completa
pytest tests/test_connection.py # só conectividade
pytest tests/test_schema.py     # só estrutura de tabelas e view
pytest tests/test_view.py       # integridade de dados da view
pytest tests/test_normalizer.py # normalização de datas/decimais (sem banco)
```

---

## Monitoramento de Progresso

Via banco de dados:

```sql
-- Status atual das execuções
SELECT * FROM cnpj.controle_sincronizacao ORDER BY data_inicio_execucao DESC LIMIT 5;

-- Arquivos com falha na última execução
SELECT nome_arquivo, status, erro_mensagem
FROM cnpj.controle_arquivos
WHERE id_execucao = (SELECT MAX(id_execucao) FROM cnpj.controle_sincronizacao)
  AND status = 'FALHA';

-- Contagem de registros por tabela
SELECT
  'empresas'         AS tabela, COUNT(*) FROM cnpj.empresas        UNION ALL
  SELECT 'estabelecimentos', COUNT(*) FROM cnpj.estabelecimentos   UNION ALL
  SELECT 'socios',           COUNT(*) FROM cnpj.socios             UNION ALL
  SELECT 'simples',          COUNT(*) FROM cnpj.simples;

-- Consulta rápida via view (com CNPJ formatado e descrições)
SELECT cnpj_formatado, razao_social, situacao_cadastral_descricao, municipio_descricao
FROM cnpj.vw_empresas_completo
WHERE cnpj_completo = '00000000000191';

-- Estatísticas por situação cadastral via view
SELECT situacao_cadastral_descricao, COUNT(*)
FROM cnpj.vw_empresas_completo
GROUP BY situacao_cadastral_descricao
ORDER BY COUNT(*) DESC;
```
