"""
Módulo de conexão com SQL Server para persistência dos dados CNPJ.

Fornece:
  - Conexão com SQL Server via pyodbc
  - Operações de bulk MERGE usando tabelas temporárias (muito mais rápido
    do que inserções linha a linha)
  - Controle idempotente de sincronização via controle_sincronizacao
  - Todas as queries parametrizadas (sem interpolação de string com dados)
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import uuid
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

import pandas as pd
import pyodbc
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.logger_enhanced import logger, structured_logger

# ---------------------------------------------------------------------------
# Auto-detecção de driver ODBC
# ---------------------------------------------------------------------------

def _best_sqlserver_driver() -> str:
    """
    Retorna o melhor driver ODBC disponível para SQL Server.
    Prioriza versões mais recentes do Microsoft ODBC Driver,
    caindo para o driver genérico "SQL Server" do Windows se necessário.
    """
    available = {d.lower(): d for d in pyodbc.drivers()}
    for candidate in (
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ):
        if candidate.lower() in available:
            return available[candidate.lower()]
    # Se não encontrar nenhum conhecido, retorna o primeiro disponível
    all_drivers = pyodbc.drivers()
    sqlserver_drivers = [d for d in all_drivers if "sql" in d.lower()]
    if sqlserver_drivers:
        return sqlserver_drivers[0]
    raise RuntimeError(
        "Nenhum driver ODBC para SQL Server encontrado. "
        "Instale 'ODBC Driver 18 for SQL Server' (Linux/Mac) "
        "ou use o driver nativo do Windows."
    )


# ---------------------------------------------------------------------------
# Conexão base
# ---------------------------------------------------------------------------

class SQLServerConnection:
    """Gerencia conexões com SQL Server."""

    def __init__(
        self,
        server: str = "72.60.4.227",
        database: str = "master",
        username: Optional[str] = None,
        password: Optional[str] = None,
        driver: Optional[str] = None,
        schema: str = "cnpj",
    ):
        self.server = server
        self.database = database
        self.driver = driver or os.getenv("DB_DRIVER") or _best_sqlserver_driver()
        self.schema = schema
        logger.debug("Driver ODBC selecionado: {}", self.driver)

        # Credenciais: parâmetro > variável de ambiente
        self.username = username or os.getenv("DB_USERNAME") or os.getenv("SQLSERVER_USERNAME")
        self.password = password or os.getenv("DB_PASSWORD") or os.getenv("SQLSERVER_PASSWORD")

    def _conn_str(self, database: Optional[str] = None) -> str:
        db = database or self.database
        base = (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={db};"
            "TrustServerCertificate=yes;"
            "Encrypt=no;"
            "ConnectRetryCount=3;"
            "ConnectRetryInterval=10;"
        )
        if self.username and self.password:
            return base + f"UID={self.username};PWD={self.password};"
        return base + "Trusted_Connection=yes;"

    @contextmanager
    def connect(self, autocommit: bool = False, timeout: int = 300) -> Generator[pyodbc.Connection, None, None]:
        """Context manager para conexão com SQL Server."""
        conn = None
        try:
            conn = pyodbc.connect(self._conn_str(), autocommit=autocommit)
            conn.timeout = timeout
            yield conn
            if not autocommit:
                conn.commit()
        except pyodbc.Error:
            if conn and not autocommit:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()

    @contextmanager
    def connect_master(self) -> Generator[pyodbc.Connection, None, None]:
        """Conexão com autocommit para DDL (CREATE DATABASE, etc.)."""
        conn = None
        try:
            conn = pyodbc.connect(self._conn_str("master"), autocommit=True)
            conn.timeout = 60
            yield conn
        finally:
            if conn:
                conn.close()

    def test_connection(self) -> bool:
        try:
            with self.connect() as conn:
                conn.cursor().execute("SELECT 1")
            return True
        except Exception as e:
            logger.error("Falha no teste de conexão: {}", e)
            return False

    def execute_query(self, query: str, params: tuple = ()) -> List[Tuple]:
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(query, params)
            return cur.fetchall()

    def execute_non_query(self, query: str, params: tuple = ()) -> int:
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(query, params)
            return cur.rowcount


# ---------------------------------------------------------------------------
# Classe especializada CNPJ
# ---------------------------------------------------------------------------

_CHUNK_SIZE = 50_000  # linhas por lote no bulk insert (fast_executemany)
_BCP_STAGE_SCHEMA = "dbo"
_BCP_FIELD_TERMINATOR = "\x1f"
_BCP_FIELD_TERMINATOR_ARG = "0x1f"
_BCP_ROW_TERMINATOR_ARG = "0x0a"


class CNPJDatabase(SQLServerConnection):
    """Operações específicas do schema CNPJ."""

    def __init__(self, database: str = "receita-federal", **kwargs):
        super().__init__(database=database, **kwargs)
        self.schema = "cnpj"
        self.load_engine = (os.getenv("DB_LOAD_ENGINE", "auto") or "auto").strip().lower()
        if self.load_engine not in {"auto", "pyodbc", "bcp"}:
            logger.warning("DB_LOAD_ENGINE inválido '{}'; usando 'auto'", self.load_engine)
            self.load_engine = "auto"
        self._bcp_path = shutil.which("bcp")
        logger.info(
            "Engine de carga SQL: {}{}",
            self.load_engine,
            f" (bcp: {self._bcp_path})" if self._bcp_path else "",
        )

    # ------------------------------------------------------------------
    # Helper: insert em chunks com fast_executemany
    # ------------------------------------------------------------------

    @staticmethod
    def _df_to_data(df: "pd.DataFrame") -> list:
        """Converte DataFrame em lista de tuplas, substituindo NA/NaN por None.

        Usa itertuples (retorna Python scalars nativos para colunas ArrowDtype),
        que mantém fast_executemany ativo no pyodbc. Abordagens baseadas em
        to_numpy/zip podem retornar pyarrow scalars que desabilitam fast_executemany
        silenciosamente, forçando insert linha-a-linha (~100x mais lento).
        """
        _pd = pd
        rows = []
        for row in df.itertuples(index=False, name=None):
            rows.append(tuple(
                None if (v is None or v is _pd.NA or (isinstance(v, float) and v != v))
                else v
                for v in row
            ))
        return rows

    @staticmethod
    def _chunked_executemany(
        cur: pyodbc.Cursor,
        sql: str,
        data: list,
        chunk_size: int = _CHUNK_SIZE,
        input_sizes: list | None = None,
    ) -> None:
        """Insere `data` em lotes de `chunk_size` via fast_executemany.

        `input_sizes` é re-aplicado antes de cada lote porque pyodbc 5.x descarta
        os tamanhos após cada chamada a executemany.
        """
        total = len(data)
        for inicio in range(0, total, chunk_size):
            chunk = data[inicio : inicio + chunk_size]
            if input_sizes:
                cur.setinputsizes(input_sizes)
            cur.executemany(sql, chunk)
            logger.debug("  inseridas {}/{} linhas", min(inicio + chunk_size, total), total)

    def _use_bcp(self, group: str) -> bool:
        if self.load_engine == "pyodbc":
            return False
        if self.load_engine == "bcp":
            if not self._bcp_path:
                raise RuntimeError("DB_LOAD_ENGINE=bcp mas bcp.exe não foi encontrado no PATH")
            return True
        return self._bcp_path is not None

    @staticmethod
    def _valid_iso_date_str(value: Any) -> Optional[str]:
        if not isinstance(value, str) or len(value) != 10:
            return None
        try:
            date.fromisoformat(value)
            return value
        except ValueError:
            return None

    @staticmethod
    def _source_projection(columns: list[str]) -> str:
        return ",\n                            ".join(
            f"NULLIF({col}, '') AS {col}" for col in columns
        )

    @staticmethod
    def _stage_table_name(prefix: str) -> str:
        return f"codex_{prefix}_{uuid.uuid4().hex[:10]}"

    def _create_stage_table(self, table_name: str, columns_with_lengths: list[tuple[str, int]]) -> None:
        columns_sql = ",\n                        ".join(
            f"{name} VARCHAR({length})" for name, length in columns_with_lengths
        )
        sql = f"""
            CREATE TABLE {_BCP_STAGE_SCHEMA}.{table_name} (
                {columns_sql}
            )
        """
        with self.connect() as conn:
            conn.cursor().execute(sql)

    def _drop_stage_table(self, table_name: str) -> None:
        try:
            with self.connect() as conn:
                conn.cursor().execute(f"DROP TABLE IF EXISTS {_BCP_STAGE_SCHEMA}.{table_name}")
        except Exception:
            pass

    def _bcp_load_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        if not self._bcp_path:
            raise RuntimeError("bcp.exe não encontrado no PATH")

        export_df = df.where(pd.notnull(df), "")
        tmp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                "w",
                delete=False,
                suffix=".csv",
                encoding="utf-8",
                newline="",
            ) as tmp:
                tmp_path = Path(tmp.name)
                export_df.to_csv(
                    tmp,
                    sep=_BCP_FIELD_TERMINATOR,
                    index=False,
                    header=False,
                    lineterminator="\n",
                )

            cmd = [
                self._bcp_path,
                f"{_BCP_STAGE_SCHEMA}.{table_name}",
                "in",
                str(tmp_path),
                "-S",
                self.server,
                "-d",
                self.database,
                "-c",
                "-C",
                "65001",
                "-t",
                _BCP_FIELD_TERMINATOR_ARG,
                "-r",
                _BCP_ROW_TERMINATOR_ARG,
                "-q",
                "-h",
                "TABLOCK",
                "-b",
                str(_CHUNK_SIZE),
                "-a",
                "65535",
            ]
            if self.username and self.password:
                cmd.extend(["-U", self.username, "-P", self.password])
            else:
                cmd.append("-T")

            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            if proc.returncode != 0:
                message = (proc.stderr or proc.stdout).strip()
                raise RuntimeError(f"BCP falhou ({proc.returncode}): {message}")
        finally:
            if tmp_path is not None:
                tmp_path.unlink(missing_ok=True)

    # ------------------------------------------------------------------
    # Inicialização
    # ------------------------------------------------------------------

    def create_database_if_not_exists(self) -> bool:
        try:
            with self.connect_master() as conn:
                cur = conn.cursor()
                cur.execute("SELECT name FROM sys.databases WHERE name = ?", (self.database,))
                if cur.fetchone():
                    logger.debug("Banco '{}' já existe", self.database)
                    return True
                cur.execute(f"CREATE DATABASE [{self.database}]")
                logger.info("Banco '{}' criado", self.database)
                # Modo SIMPLE reduz uso de log; sem AUTO_SHRINK (causa fragmentação)
                cur.execute(f"ALTER DATABASE [{self.database}] SET RECOVERY SIMPLE")
                return True
        except Exception as e:
            logger.error("Erro ao criar banco: {}", e)
            return False

    def execute_schema_script(self, script_path: Path) -> bool:
        """Executa um script SQL dividindo nos separadores GO."""
        try:
            script = script_path.read_text(encoding="utf-8")
            # Dividir por GO (linha isolada, case-insensitive)
            commands = re.split(r"^\s*GO\s*$", script, flags=re.MULTILINE | re.IGNORECASE)

            with self.connect(autocommit=True) as conn:
                cur = conn.cursor()
                for i, cmd in enumerate(commands, 1):
                    cmd = cmd.strip()
                    if not cmd:
                        continue
                    try:
                        cur.execute(cmd)
                    except pyodbc.Error as e:
                        logger.error("Erro no bloco {} do script: {}", i, e)
                        logger.debug("Comando: {}", cmd[:300])
                        raise

            logger.debug("Script {} executado com sucesso", script_path.name)
            return True
        except Exception as e:
            logger.error("Erro ao executar script: {}", e)
            return False

    # ------------------------------------------------------------------
    # Controle de sincronização
    # ------------------------------------------------------------------

    def check_snapshot_exists(self, snapshot_date: date) -> bool:
        """Retorna True se snapshot já foi processado com SUCESSO."""
        try:
            rows = self.execute_query(
                f"SELECT 1 FROM {self.schema}.controle_sincronizacao "
                "WHERE snapshot_date = ? AND status = 'SUCESSO'",
                (str(snapshot_date),),
            )
            return bool(rows)
        except Exception as e:
            logger.error("Erro ao verificar snapshot: {}", e)
            return False

    def is_snapshot_running(self, snapshot_date: date) -> bool:
        """Retorna True se snapshot já está em execução (evita corridas)."""
        try:
            rows = self.execute_query(
                f"SELECT 1 FROM {self.schema}.controle_sincronizacao "
                "WHERE snapshot_date = ? AND status = 'EM_EXECUCAO'",
                (str(snapshot_date),),
            )
            return bool(rows)
        except Exception as e:
            logger.error("Erro ao verificar execução em andamento: {}", e)
            return False

    def start_sync_session(self, snapshot_date: date, force: bool = False) -> Optional[int]:
        """Registra início de uma execução. Retorna id_execucao ou None.
        
        Se force=True, qualquer execução travada em EM_EXECUCAO para esta data
        é marcada como FALHA antes de criar a nova sessão.
        """
        snap_str = str(snapshot_date)
        try:
            with self.connect() as conn:
                cur = conn.cursor()

                if force:
                    # Limpa registros travados em EM_EXECUCAO para este snapshot
                    cur.execute(
                        f"UPDATE {self.schema}.controle_sincronizacao "
                        "SET status = 'FALHA', data_fim_execucao = GETDATE(), "
                        "erro_mensagem = 'Interrompida por --force' "
                        "WHERE snapshot_date = ? AND status = 'EM_EXECUCAO'",
                        (snap_str,),
                    )

                cur.execute(
                    f"INSERT INTO {self.schema}.controle_sincronizacao "
                    "(snapshot_date, status, data_inicio_execucao) "
                    "VALUES (?, 'EM_EXECUCAO', GETDATE())",
                    (snap_str,),
                )
                cur.execute(
                    f"SELECT MAX(id_execucao) FROM {self.schema}.controle_sincronizacao "
                    "WHERE snapshot_date = ?",
                    (snap_str,),
                )
                row = cur.fetchone()
                if row and row[0] is not None:
                    exec_id = int(row[0])
                    logger.debug("Sessão iniciada: id_execucao={}, snapshot={}", exec_id, snapshot_date)
                    return exec_id
                logger.error("Não foi possível obter id_execucao após INSERT")
                return None
        except Exception as e:
            logger.error("Erro ao iniciar sessão: {}", e)
            return None

    def update_sync_session(
        self,
        exec_id: int,
        status: str,
        total_files: Optional[int] = None,
        processed_files: Optional[int] = None,
        failed_files: Optional[int] = None,
        total_records: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> bool:
        try:
            sets = ["status = ?"]
            params: List[Any] = [status]

            if total_files is not None:
                sets.append("total_arquivos = ?"); params.append(total_files)
            if processed_files is not None:
                sets.append("arquivos_processados = ?"); params.append(processed_files)
            if failed_files is not None:
                sets.append("arquivos_falha = ?"); params.append(failed_files)
            if total_records is not None:
                sets.append("total_registros = ?"); params.append(total_records)
            if status in ("SUCESSO", "FALHA", "CANCELADO"):
                sets.append("data_fim_execucao = GETDATE()")
                sets.append("duracao_segundos = DATEDIFF(SECOND, data_inicio_execucao, GETDATE())")
            if error_message:
                sets.append("erro_mensagem = ?"); params.append(str(error_message)[:4000])

            params.append(exec_id)
            self.execute_non_query(
                f"UPDATE {self.schema}.controle_sincronizacao "
                f"SET {', '.join(sets)} WHERE id_execucao = ?",
                tuple(params),
            )
            logger.info("Sessão {} → {}", exec_id, status)
            return True
        except Exception as e:
            logger.error("Erro ao atualizar sessão: {}", e)
            return False

    def add_file_to_sync(self, exec_id: int, group: str, filename: str, status: str = "PENDENTE") -> Optional[int]:
        try:
            with self.connect() as conn:
                cur = conn.cursor()
                cur.execute(
                    f"INSERT INTO {self.schema}.controle_arquivos "
                    "(id_execucao, grupo_arquivo, nome_arquivo, status, data_inicio) "
                    "VALUES (?, ?, ?, ?, GETDATE())",
                    (exec_id, group, filename, status),
                )
                cur.execute("SELECT SCOPE_IDENTITY()")
                row = cur.fetchone()
                return int(row[0]) if row and row[0] is not None else None
        except Exception as e:
            logger.error("Erro ao registrar arquivo {}: {}", filename, e)
            return None

    def update_file_status(
        self,
        file_id: int,
        status: str,
        total_records: Optional[int] = None,
        invalid_records: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> bool:
        try:
            sets = ["status = ?", "data_fim = GETDATE()"]
            params: List[Any] = [status]
            if total_records is not None:
                sets.append("total_registros = ?"); params.append(total_records)
            if invalid_records is not None:
                sets.append("registros_invalidos = ?"); params.append(invalid_records)
            if error_message:
                sets.append("erro_mensagem = ?"); params.append(str(error_message)[:2000])
            params.append(file_id)
            self.execute_non_query(
                f"UPDATE {self.schema}.controle_arquivos "
                f"SET {', '.join(sets)} WHERE id_arquivo = ?",
                tuple(params),
            )
            return True
        except Exception as e:
            logger.error("Erro ao atualizar arquivo {}: {}", file_id, e)
            return False

    # ------------------------------------------------------------------
    # Bulk MERGE — tabelas de referência (dimensões pequenas)
    # ------------------------------------------------------------------

    def bulk_upsert_reference(self, table: str, df: pd.DataFrame, snapshot_date: date) -> int:
        """
        MERGE idempotente para tabelas de referência (cnaes, motivos, etc.).
        Usa tabela temporária + MERGE para eficiência.
        Retorna número de linhas afetadas (inserted + updated).
        """
        if df.empty:
            return 0
        if self._use_bcp(table):
            return self._bulk_upsert_reference_bcp(table, df, snapshot_date)

        # Garantir colunas limpas
        df = df[["codigo", "descricao"]].copy()
        df["codigo"] = df["codigo"].astype(str).str.strip()
        df["descricao"] = df["descricao"].fillna("").astype(str).str.strip()
        df = df[df["codigo"].notna() & (df["codigo"] != "") & (df["codigo"] != "nan")]

        # Deduplicação Python — MERGE falha se a fonte tiver chaves repetidas
        duplicados = df.duplicated(subset="codigo", keep=False).sum()
        if duplicados:
            logger.warning(
                "{} linhas duplicadas em 'codigo' em {} — mantendo última ocorrência",
                duplicados, table,
            )
            df = df.drop_duplicates(subset="codigo", keep="last").reset_index(drop=True)
            logger.info("{} linhas únicas após deduplicação", len(df))

        data = CNPJDatabase._df_to_data(df)
        if not data:
            return 0

        target = f"{self.schema}.{table}"
        try:
            with self.connect() as conn:
                cur = conn.cursor()
                cur.fast_executemany = "ODBC Driver" in self.driver

                cur.execute(
                    "CREATE TABLE #tmp_ref (codigo NVARCHAR(20) NOT NULL, descricao NVARCHAR(255) NOT NULL)"
                )
                self._chunked_executemany(cur, "INSERT INTO #tmp_ref VALUES (?, ?)", data)

                # Índice clusterizado ANTES do MERGE — garante seek em vez de full scan
                cur.execute("CREATE CLUSTERED INDEX IX_tmp_ref_codigo ON #tmp_ref (codigo)")

                snap_str = str(snapshot_date)
                # LTRIM/RTRIM + GROUP BY no USING: garante unicidade mesmo quando o
                # arquivo-fonte tem caracteres invisíveis que Python não detecta como iguais.
                cur.execute(f"""
                    MERGE {target} AS t
                    USING (
                        SELECT
                            LTRIM(RTRIM(codigo))         AS codigo,
                            MAX(LTRIM(RTRIM(descricao))) AS descricao
                        FROM #tmp_ref
                        GROUP BY LTRIM(RTRIM(codigo))
                    ) AS s ON t.codigo = s.codigo
                    WHEN MATCHED AND t.descricao <> s.descricao THEN
                        UPDATE SET
                            t.descricao     = s.descricao,
                            t.snapshot_date = ?,
                            t.data_carga    = GETDATE()
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT (codigo, descricao, snapshot_date, data_carga)
                        VALUES (s.codigo, s.descricao, ?, GETDATE());
                """, (snap_str, snap_str))

                affected = cur.rowcount
                cur.execute("DROP TABLE IF EXISTS #tmp_ref")
                logger.info("{}: {} registros mesclados", table, affected)
                return affected
        except Exception as e:
            logger.error("Erro no bulk_upsert_reference [{}]: {}", table, e)
            raise

    def _prepare_reference_bcp_df(self, df: pd.DataFrame, snapshot_date: date) -> pd.DataFrame:
        staged = df[["codigo", "descricao"]].copy()
        staged["codigo"] = staged["codigo"].astype(str).str.strip()
        staged["descricao"] = staged["descricao"].fillna("").astype(str).str.strip()
        staged = staged[
            staged["codigo"].notna()
            & (staged["codigo"] != "")
            & (staged["codigo"] != "nan")
        ]

        duplicados = staged.duplicated(subset="codigo", keep=False).sum()
        if duplicados:
            logger.warning(
                "{} linhas duplicadas em 'codigo' em staging BCP de {} — mantendo última ocorrência",
                duplicados,
                "referencia",
            )
            staged = staged.drop_duplicates(subset="codigo", keep="last").reset_index(drop=True)

        staged = staged.astype(object).where(pd.notnull(staged), None)
        staged["codigo"] = staged["codigo"].apply(lambda v: str(v) if pd.notna(v) else None)
        staged["descricao"] = staged["descricao"].apply(lambda v: str(v) if pd.notna(v) else "")
        staged["snapshot_date"] = str(snapshot_date)

        limits = {"codigo": 20, "descricao": 255, "snapshot_date": 10}
        for col, max_len in limits.items():
            staged[col] = staged[col].apply(
                lambda v, m=max_len: v[:m] if isinstance(v, str) and len(v) > m else v
            )
        return staged[["codigo", "descricao", "snapshot_date"]]

    def _bulk_upsert_reference_bcp(self, table: str, df: pd.DataFrame, snapshot_date: date) -> int:
        staged = self._prepare_reference_bcp_df(df, snapshot_date)
        if staged.empty:
            return 0

        table_name = self._stage_table_name(f"{table}_ref")
        columns = [("codigo", 20), ("descricao", 255), ("snapshot_date", 10)]

        try:
            self._create_stage_table(table_name, columns)
            self._bcp_load_dataframe(staged, table_name)
            with self.connect() as conn:
                cur = conn.cursor()
                cur.execute(
                    f"CREATE CLUSTERED INDEX IX_{table_name}_codigo ON "
                    f"{_BCP_STAGE_SCHEMA}.{table_name} (codigo)"
                )
                cur.execute(f"""
                    MERGE {self.schema}.{table} AS t
                    USING (
                        SELECT
                            LTRIM(RTRIM(codigo)) AS codigo,
                            MAX(LTRIM(RTRIM(descricao))) AS descricao,
                            MAX(snapshot_date) AS snapshot_date
                        FROM {_BCP_STAGE_SCHEMA}.{table_name}
                        WHERE NULLIF(LTRIM(RTRIM(codigo)), '') IS NOT NULL
                        GROUP BY LTRIM(RTRIM(codigo))
                    ) AS s ON t.codigo = s.codigo
                    WHEN MATCHED AND ISNULL(t.descricao, '') <> ISNULL(s.descricao, '') THEN
                        UPDATE SET
                            descricao = s.descricao,
                            snapshot_date = s.snapshot_date,
                            data_carga = GETDATE()
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT (codigo, descricao, snapshot_date, data_carga)
                        VALUES (s.codigo, s.descricao, s.snapshot_date, GETDATE())
                    OUTPUT $action;
                """)
                actions = [row[0] for row in cur.fetchall()]
                cur.execute(f"DROP TABLE IF EXISTS {_BCP_STAGE_SCHEMA}.{table_name}")
                affected = actions.count("INSERT") + actions.count("UPDATE")
                logger.info("{}: {} registros mesclados", table, affected)
                return affected
        finally:
            self._drop_stage_table(table_name)

    # ------------------------------------------------------------------
    # Bulk MERGE — empresas
    # ------------------------------------------------------------------

    def bulk_upsert_empresas(self, df: pd.DataFrame, snapshot_date: date) -> Tuple[int, int]:
        """
        MERGE idempotente para cnpj.empresas.
        Retorna (inseridos, atualizados).
        """
        if df.empty:
            return 0, 0
        if self._use_bcp("empresas"):
            return self._bulk_upsert_empresas_bcp(df, snapshot_date)

        cols = [
            "cnpj_basico", "razao_social", "natureza_juridica",
            "qualificacao_responsavel", "capital_social",
            "porte_empresa", "ente_federativo_responsavel",
        ]
        df = df[cols].copy()
        df["cnpj_basico"] = df["cnpj_basico"].astype(str).str.strip().str.zfill(8)
        df = df[df["cnpj_basico"].notna() & (df["cnpj_basico"] != "") & (df["cnpj_basico"] != "nan")]
        # Garante que capital_social é numérico — strings vazias/inválidas viram None
        df["capital_social"] = pd.to_numeric(df["capital_social"], errors="coerce")
        # .astype(object) garante que NaN/None sejam Python None (fast_executemany não aceita float nan)
        df = df.astype(object).where(pd.notnull(df), None)
        # float nativo evita que numpy.float64 residual seja serializado como string pelo fast_executemany
        df["capital_social"] = [float(v) if v is not None else None for v in df["capital_social"]]

        # Deduplicação: duplicatas intrachunk causam falha no CREATE CLUSTERED INDEX
        duplicados = df.duplicated(subset="cnpj_basico", keep=False).sum()
        if duplicados:
            logger.warning(
                "bulk_upsert_empresas: {} linhas duplicadas em cnpj_basico — mantendo última ocorrência",
                duplicados,
            )
            df = df.drop_duplicates(subset="cnpj_basico", keep="last").reset_index(drop=True)

        snap_str = str(snapshot_date)
        data = [(*row, snap_str) for row in df.itertuples(index=False, name=None)]
        if not data:
            return 0, 0

        # Tipos explícitos para fast_executemany: sem isso, capital_social=None nas primeiras
        # linhas faz o driver inferir VARCHAR e quebrar com "varchar to numeric" depois.
        _input_sizes = [
            (pyodbc.SQL_VARCHAR,  8,   0),   # cnpj_basico
            (pyodbc.SQL_VARCHAR,  150, 0),   # razao_social
            (pyodbc.SQL_VARCHAR,  4,   0),   # natureza_juridica
            (pyodbc.SQL_VARCHAR,  2,   0),   # qualificacao_responsavel
            (pyodbc.SQL_DECIMAL,  18,  2),   # capital_social
            (pyodbc.SQL_VARCHAR,  2,   0),   # porte_empresa
            (pyodbc.SQL_VARCHAR,  50,  0),   # ente_federativo_responsavel
            (pyodbc.SQL_VARCHAR,  10,  0),   # snapshot_date
        ]

        target = f"{self.schema}.empresas"
        try:
            with self.connect() as conn:
                cur = conn.cursor()
                cur.fast_executemany = "ODBC Driver" in self.driver
                cur.setinputsizes(_input_sizes)

                cur.execute("""
                    CREATE TABLE #tmp_emp (
                        cnpj_basico                 VARCHAR(8),
                        razao_social                VARCHAR(150),
                        natureza_juridica           VARCHAR(4),
                        qualificacao_responsavel    VARCHAR(2),
                        capital_social              DECIMAL(18,2),
                        porte_empresa               VARCHAR(2),
                        ente_federativo_responsavel VARCHAR(50),
                        snapshot_date               VARCHAR(10)
                    )
                """)
                self._chunked_executemany(cur, "INSERT INTO #tmp_emp VALUES (?,?,?,?,?,?,?,?)", data)

                # Índice clusterizado ANTES do MERGE
                cur.execute("CREATE CLUSTERED INDEX IX_tmp_emp ON #tmp_emp (cnpj_basico)")

                # Versão otimizada: sempre atualizar quando houver match
                # (mais rápido que verificar cada campo individualmente)
                cur.execute(f"""
                    MERGE {target} AS t
                    USING #tmp_emp AS s ON t.cnpj_basico = s.cnpj_basico
                    WHEN MATCHED THEN
                        UPDATE SET
                            razao_social                = s.razao_social,
                            natureza_juridica           = s.natureza_juridica,
                            qualificacao_responsavel    = s.qualificacao_responsavel,
                            capital_social              = s.capital_social,
                            porte_empresa               = s.porte_empresa,
                            ente_federativo_responsavel = s.ente_federativo_responsavel,
                            snapshot_date               = s.snapshot_date,
                            data_carga                  = GETDATE()
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT (cnpj_basico, razao_social, natureza_juridica,
                                qualificacao_responsavel, capital_social,
                                porte_empresa, ente_federativo_responsavel,
                                snapshot_date, data_carga)
                        VALUES (s.cnpj_basico, s.razao_social, s.natureza_juridica,
                                s.qualificacao_responsavel, s.capital_social,
                                s.porte_empresa, s.ente_federativo_responsavel,
                                s.snapshot_date, GETDATE())
                    OUTPUT $action;
                """)

                actions = [r[0] for r in cur.fetchall()]
                inserted = actions.count("INSERT")
                updated = actions.count("UPDATE")
                cur.execute("DROP TABLE IF EXISTS #tmp_emp")
                return inserted, updated
        except Exception as e:
            logger.error("Erro no bulk_upsert_empresas: {}", e)
            raise

    def _prepare_empresas_bcp_df(self, df: pd.DataFrame, snapshot_date: date) -> pd.DataFrame:
        limits = {
            "cnpj_basico": 8,
            "razao_social": 150,
            "natureza_juridica": 4,
            "qualificacao_responsavel": 2,
            "capital_social": 32,
            "porte_empresa": 2,
            "ente_federativo_responsavel": 50,
        }
        staged = df[list(limits)].copy()
        staged["cnpj_basico"] = staged["cnpj_basico"].astype(str).str.strip().str.zfill(8)
        staged = staged[
            staged["cnpj_basico"].notna()
            & (staged["cnpj_basico"] != "")
            & (staged["cnpj_basico"] != "nan")
        ]
        staged["capital_social"] = pd.to_numeric(staged["capital_social"], errors="coerce")
        staged = staged.astype(object).where(pd.notnull(staged), None)

        duplicados = staged.duplicated(subset="cnpj_basico", keep=False).sum()
        if duplicados:
            logger.warning(
                "bulk_upsert_empresas BCP: {} linhas duplicadas em cnpj_basico — mantendo última ocorrência",
                duplicados,
            )
            staged = staged.drop_duplicates(subset="cnpj_basico", keep="last").reset_index(drop=True)

        for col in staged.columns:
            if col == "capital_social":
                staged[col] = staged[col].apply(
                    lambda v: f"{float(v):.2f}" if v is not None else None
                )
            else:
                staged[col] = staged[col].apply(lambda v: str(v) if pd.notna(v) else None)

        staged["snapshot_date"] = str(snapshot_date)
        for col, max_len in {**limits, "snapshot_date": 10}.items():
            staged[col] = staged[col].apply(
                lambda v, m=max_len: v[:m] if isinstance(v, str) and len(v) > m else v
            )
        return staged[list(limits) + ["snapshot_date"]]

    def _bulk_upsert_empresas_bcp(self, df: pd.DataFrame, snapshot_date: date) -> Tuple[int, int]:
        staged = self._prepare_empresas_bcp_df(df, snapshot_date)
        if staged.empty:
            return 0, 0

        columns = [
            ("cnpj_basico", 8),
            ("razao_social", 150),
            ("natureza_juridica", 4),
            ("qualificacao_responsavel", 2),
            ("capital_social", 32),
            ("porte_empresa", 2),
            ("ente_federativo_responsavel", 50),
            ("snapshot_date", 10),
        ]
        table_name = self._stage_table_name("empresas")

        try:
            self._create_stage_table(table_name, columns)
            self._bcp_load_dataframe(staged, table_name)
            with self.connect() as conn:
                cur = conn.cursor()
                cur.execute(
                    f"CREATE CLUSTERED INDEX IX_{table_name}_cnpj ON "
                    f"{_BCP_STAGE_SCHEMA}.{table_name} (cnpj_basico)"
                )
                cur.execute(f"""
                    MERGE {self.schema}.empresas AS t
                    USING (
                        SELECT
                            NULLIF(cnpj_basico, '') AS cnpj_basico,
                            NULLIF(razao_social, '') AS razao_social,
                            NULLIF(natureza_juridica, '') AS natureza_juridica,
                            NULLIF(qualificacao_responsavel, '') AS qualificacao_responsavel,
                            TRY_CONVERT(DECIMAL(18,2), NULLIF(capital_social, '')) AS capital_social,
                            NULLIF(porte_empresa, '') AS porte_empresa,
                            NULLIF(ente_federativo_responsavel, '') AS ente_federativo_responsavel,
                            NULLIF(snapshot_date, '') AS snapshot_date
                        FROM {_BCP_STAGE_SCHEMA}.{table_name}
                    ) AS s ON t.cnpj_basico = s.cnpj_basico
                    WHEN MATCHED THEN
                        UPDATE SET
                            razao_social = s.razao_social,
                            natureza_juridica = s.natureza_juridica,
                            qualificacao_responsavel = s.qualificacao_responsavel,
                            capital_social = s.capital_social,
                            porte_empresa = s.porte_empresa,
                            ente_federativo_responsavel = s.ente_federativo_responsavel,
                            snapshot_date = s.snapshot_date,
                            data_carga = GETDATE()
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT (
                            cnpj_basico, razao_social, natureza_juridica,
                            qualificacao_responsavel, capital_social, porte_empresa,
                            ente_federativo_responsavel, snapshot_date, data_carga
                        )
                        VALUES (
                            s.cnpj_basico, s.razao_social, s.natureza_juridica,
                            s.qualificacao_responsavel, s.capital_social, s.porte_empresa,
                            s.ente_federativo_responsavel, s.snapshot_date, GETDATE()
                        )
                    OUTPUT $action;
                """)
                actions = [row[0] for row in cur.fetchall()]
                cur.execute(f"DROP TABLE IF EXISTS {_BCP_STAGE_SCHEMA}.{table_name}")
                return actions.count("INSERT"), actions.count("UPDATE")
        finally:
            self._drop_stage_table(table_name)

    def _prepare_estabelecimentos_bcp_df(self, df: pd.DataFrame, snapshot_date: date) -> pd.DataFrame:
        limits = {
            "cnpj_completo": 14,
            "cnpj_basico": 8,
            "cnpj_ordem": 4,
            "cnpj_dv": 2,
            "identificador_matriz_filial": 1,
            "nome_fantasia": 55,
            "situacao_cadastral": 2,
            "data_situacao_cadastral": 10,
            "motivo_situacao_cadastral": 2,
            "nome_cidade_exterior": 55,
            "pais": 3,
            "data_inicio_atividade": 10,
            "cnae_fiscal": 10,
            "cnae_fiscal_secundaria": 1000,
            "tipo_logradouro": 20,
            "logradouro": 60,
            "numero": 6,
            "complemento": 156,
            "bairro": 50,
            "cep": 8,
            "uf": 2,
            "municipio": 4,
            "ddd_1": 4,
            "telefone_1": 9,
            "ddd_2": 4,
            "telefone_2": 9,
            "ddd_fax": 4,
            "fax": 9,
            "correio_eletronico": 115,
            "situacao_especial": 23,
            "data_situacao_especial": 10,
        }
        required = [col for col in limits if col != "cnpj_completo"]
        staged = df[required].copy()
        staged["cnpj_completo"] = (
            staged["cnpj_basico"].astype(str).str.strip().str.zfill(8)
            + staged["cnpj_ordem"].astype(str).str.strip().str.zfill(4)
            + staged["cnpj_dv"].astype(str).str.strip().str.zfill(2)
        )
        staged = staged[staged["cnpj_completo"].str.len() == 14]
        staged["snapshot_date"] = str(snapshot_date)
        staged = staged[list(limits) + ["snapshot_date"]]
        staged = staged.astype(object).where(pd.notnull(staged), None)
        for col in staged.columns:
            staged[col] = staged[col].apply(lambda v: str(v) if pd.notna(v) else None)
        for col in ("data_situacao_cadastral", "data_inicio_atividade", "data_situacao_especial"):
            staged[col] = staged[col].apply(self._valid_iso_date_str)
        for col, max_len in limits.items():
            staged[col] = staged[col].apply(
                lambda v, m=max_len: v[:m] if isinstance(v, str) and len(v) > m else v
            )
        return staged

    def _bulk_upsert_estabelecimentos_bcp(self, df: pd.DataFrame, snapshot_date: date) -> Tuple[int, int]:
        staged = self._prepare_estabelecimentos_bcp_df(df, snapshot_date)
        if staged.empty:
            return 0, 0

        limits = {
            "cnpj_completo": 14,
            "cnpj_basico": 8,
            "cnpj_ordem": 4,
            "cnpj_dv": 2,
            "identificador_matriz_filial": 1,
            "nome_fantasia": 55,
            "situacao_cadastral": 2,
            "data_situacao_cadastral": 10,
            "motivo_situacao_cadastral": 2,
            "nome_cidade_exterior": 55,
            "pais": 3,
            "data_inicio_atividade": 10,
            "cnae_fiscal": 10,
            "cnae_fiscal_secundaria": 1000,
            "tipo_logradouro": 20,
            "logradouro": 60,
            "numero": 6,
            "complemento": 156,
            "bairro": 50,
            "cep": 8,
            "uf": 2,
            "municipio": 4,
            "ddd_1": 4,
            "telefone_1": 9,
            "ddd_2": 4,
            "telefone_2": 9,
            "ddd_fax": 4,
            "fax": 9,
            "correio_eletronico": 115,
            "situacao_especial": 23,
            "data_situacao_especial": 10,
        }
        table_name = self._stage_table_name("estab")
        columns = [(col, limits[col]) for col in limits] + [("snapshot_date", 10)]
        source_projection = self._source_projection([name for name, _ in columns])

        try:
            self._create_stage_table(table_name, columns)
            self._bcp_load_dataframe(staged, table_name)
            with self.connect() as conn:
                cur = conn.cursor()
                cur.execute(
                    f"CREATE CLUSTERED INDEX IX_{table_name}_cnpj ON "
                    f"{_BCP_STAGE_SCHEMA}.{table_name} (cnpj_completo)"
                )
                cur.execute(f"""
                    MERGE {self.schema}.estabelecimentos AS t
                    USING (
                        SELECT
                            {source_projection}
                        FROM {_BCP_STAGE_SCHEMA}.{table_name}
                    ) AS s ON t.cnpj_completo = s.cnpj_completo
                    WHEN MATCHED AND (
                        ISNULL(t.situacao_cadastral,'')        <> ISNULL(s.situacao_cadastral,'') OR
                        ISNULL(CAST(t.data_situacao_cadastral AS VARCHAR),'')
                            <> ISNULL(CAST(s.data_situacao_cadastral AS VARCHAR),'') OR
                        ISNULL(t.nome_fantasia,'')             <> ISNULL(s.nome_fantasia,'') OR
                        ISNULL(t.cnae_fiscal,'')               <> ISNULL(s.cnae_fiscal,'') OR
                        ISNULL(t.motivo_situacao_cadastral,'') <> ISNULL(s.motivo_situacao_cadastral,'') OR
                        ISNULL(t.logradouro,'')                <> ISNULL(s.logradouro,'') OR
                        ISNULL(t.correio_eletronico,'')        <> ISNULL(s.correio_eletronico,'')
                    ) THEN
                        UPDATE SET
                            cnpj_basico                 = s.cnpj_basico,
                            cnpj_ordem                  = s.cnpj_ordem,
                            cnpj_dv                     = s.cnpj_dv,
                            identificador_matriz_filial = s.identificador_matriz_filial,
                            nome_fantasia               = s.nome_fantasia,
                            situacao_cadastral          = s.situacao_cadastral,
                            data_situacao_cadastral     = s.data_situacao_cadastral,
                            motivo_situacao_cadastral   = s.motivo_situacao_cadastral,
                            nome_cidade_exterior        = s.nome_cidade_exterior,
                            pais                        = s.pais,
                            data_inicio_atividade       = s.data_inicio_atividade,
                            cnae_fiscal                 = s.cnae_fiscal,
                            cnae_fiscal_secundaria      = s.cnae_fiscal_secundaria,
                            tipo_logradouro             = s.tipo_logradouro,
                            logradouro                  = s.logradouro,
                            numero                      = s.numero,
                            complemento                 = s.complemento,
                            bairro                      = s.bairro,
                            cep                         = s.cep,
                            uf                          = s.uf,
                            municipio                   = s.municipio,
                            ddd_1                       = s.ddd_1,
                            telefone_1                  = s.telefone_1,
                            ddd_2                       = s.ddd_2,
                            telefone_2                  = s.telefone_2,
                            ddd_fax                     = s.ddd_fax,
                            fax                         = s.fax,
                            correio_eletronico          = s.correio_eletronico,
                            situacao_especial           = s.situacao_especial,
                            data_situacao_especial      = s.data_situacao_especial,
                            snapshot_date               = s.snapshot_date,
                            data_carga                  = GETDATE()
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT (
                            cnpj_completo, cnpj_basico, cnpj_ordem, cnpj_dv,
                            identificador_matriz_filial, nome_fantasia, situacao_cadastral,
                            data_situacao_cadastral, motivo_situacao_cadastral,
                            nome_cidade_exterior, pais, data_inicio_atividade,
                            cnae_fiscal, cnae_fiscal_secundaria, tipo_logradouro,
                            logradouro, numero, complemento, bairro, cep, uf, municipio,
                            ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax, fax,
                            correio_eletronico, situacao_especial, data_situacao_especial,
                            snapshot_date, data_carga
                        )
                        VALUES (
                            s.cnpj_completo, s.cnpj_basico, s.cnpj_ordem, s.cnpj_dv,
                            s.identificador_matriz_filial, s.nome_fantasia, s.situacao_cadastral,
                            s.data_situacao_cadastral, s.motivo_situacao_cadastral,
                            s.nome_cidade_exterior, s.pais, s.data_inicio_atividade,
                            s.cnae_fiscal, s.cnae_fiscal_secundaria, s.tipo_logradouro,
                            s.logradouro, s.numero, s.complemento, s.bairro, s.cep,
                            s.uf, s.municipio,
                            s.ddd_1, s.telefone_1, s.ddd_2, s.telefone_2,
                            s.ddd_fax, s.fax,
                            s.correio_eletronico, s.situacao_especial, s.data_situacao_especial,
                            s.snapshot_date, GETDATE()
                        )
                    OUTPUT $action;
                """)
                actions = [row[0] for row in cur.fetchall()]
                cur.execute(f"DROP TABLE IF EXISTS {_BCP_STAGE_SCHEMA}.{table_name}")
                return actions.count("INSERT"), actions.count("UPDATE")
        finally:
            self._drop_stage_table(table_name)

    def _prepare_simples_bcp_df(self, df: pd.DataFrame, snapshot_date: date) -> pd.DataFrame:
        limits = {
            "cnpj_basico": 8,
            "opcao_pelo_simples": 1,
            "data_opcao_simples": 10,
            "data_exclusao_simples": 10,
            "opcao_mei": 1,
            "data_opcao_mei": 10,
            "data_exclusao_mei": 10,
        }
        staged = df[list(limits)].copy()
        staged["cnpj_basico"] = staged["cnpj_basico"].astype(str).str.strip().str.zfill(8)
        staged = staged[
            staged["cnpj_basico"].notna()
            & (staged["cnpj_basico"] != "")
            & (staged["cnpj_basico"] != "nan")
        ]
        staged = staged.astype(object).where(pd.notnull(staged), None)
        for col in ("data_opcao_simples", "data_exclusao_simples", "data_opcao_mei", "data_exclusao_mei"):
            staged[col] = staged[col].apply(self._valid_iso_date_str)
        for col in limits:
            staged[col] = staged[col].apply(lambda v: str(v) if pd.notna(v) else None)
        staged["snapshot_date"] = str(snapshot_date)
        for col, max_len in limits.items():
            staged[col] = staged[col].apply(
                lambda v, m=max_len: v[:m] if isinstance(v, str) and len(v) > m else v
            )
        return staged[list(limits) + ["snapshot_date"]]

    def _bulk_upsert_simples_bcp(self, df: pd.DataFrame, snapshot_date: date) -> Tuple[int, int]:
        staged = self._prepare_simples_bcp_df(df, snapshot_date)
        if staged.empty:
            return 0, 0

        limits = {
            "cnpj_basico": 8,
            "opcao_pelo_simples": 1,
            "data_opcao_simples": 10,
            "data_exclusao_simples": 10,
            "opcao_mei": 1,
            "data_opcao_mei": 10,
            "data_exclusao_mei": 10,
        }
        table_name = self._stage_table_name("simples")
        columns = [(col, limits[col]) for col in limits] + [("snapshot_date", 10)]
        source_projection = self._source_projection([name for name, _ in columns])

        try:
            self._create_stage_table(table_name, columns)
            self._bcp_load_dataframe(staged, table_name)
            with self.connect() as conn:
                cur = conn.cursor()
                cur.execute(
                    f"CREATE CLUSTERED INDEX IX_{table_name}_cnpj ON "
                    f"{_BCP_STAGE_SCHEMA}.{table_name} (cnpj_basico)"
                )
                cur.execute(f"""
                    MERGE {self.schema}.simples AS t
                    USING (
                        SELECT
                            {source_projection}
                        FROM {_BCP_STAGE_SCHEMA}.{table_name}
                    ) AS s ON t.cnpj_basico = s.cnpj_basico
                    WHEN MATCHED AND (
                        ISNULL(t.opcao_pelo_simples,'') <> ISNULL(s.opcao_pelo_simples,'') OR
                        ISNULL(t.opcao_mei,'')          <> ISNULL(s.opcao_mei,'')
                    ) THEN
                        UPDATE SET
                            opcao_pelo_simples    = s.opcao_pelo_simples,
                            data_opcao_simples    = s.data_opcao_simples,
                            data_exclusao_simples = s.data_exclusao_simples,
                            opcao_mei             = s.opcao_mei,
                            data_opcao_mei        = s.data_opcao_mei,
                            data_exclusao_mei     = s.data_exclusao_mei,
                            snapshot_date         = s.snapshot_date,
                            data_carga            = GETDATE()
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT (
                            cnpj_basico, opcao_pelo_simples,
                            data_opcao_simples, data_exclusao_simples,
                            opcao_mei, data_opcao_mei, data_exclusao_mei,
                            snapshot_date, data_carga
                        )
                        VALUES (
                            s.cnpj_basico, s.opcao_pelo_simples,
                            s.data_opcao_simples, s.data_exclusao_simples,
                            s.opcao_mei, s.data_opcao_mei, s.data_exclusao_mei,
                            s.snapshot_date, GETDATE()
                        )
                    OUTPUT $action;
                """)
                actions = [row[0] for row in cur.fetchall()]
                cur.execute(f"DROP TABLE IF EXISTS {_BCP_STAGE_SCHEMA}.{table_name}")
                return actions.count("INSERT"), actions.count("UPDATE")
        finally:
            self._drop_stage_table(table_name)

    def _prepare_socios_bcp_df(self, df: pd.DataFrame, snapshot_date: date) -> pd.DataFrame:
        limits = {
            "cnpj_basico": 8,
            "identificador_socio": 1,
            "nome_socio_razao_social": 150,
            "cnpj_cpf_socio": 14,
            "qualificacao_socio": 2,
            "data_entrada_sociedade": 10,
            "pais": 3,
            "representante_legal": 11,
            "nome_representante": 60,
            "qualificacao_representante_legal": 2,
            "faixa_etaria": 1,
        }
        cols = list(limits)
        staged = df[cols].copy()
        staged["cnpj_basico"] = staged["cnpj_basico"].astype(str).str.strip().str.zfill(8)
        staged = staged[
            staged["cnpj_basico"].notna()
            & (staged["cnpj_basico"] != "")
            & (staged["cnpj_basico"] != "nan")
        ]
        staged = staged.astype(object).where(pd.notnull(staged), None)
        for col in limits:
            if col == "data_entrada_sociedade":
                staged[col] = staged[col].apply(self._valid_iso_date_str)
            else:
                staged[col] = staged[col].apply(lambda v: str(v) if pd.notna(v) else None)
        staged["snapshot_date"] = str(snapshot_date)
        for col, max_len in limits.items():
            staged[col] = staged[col].apply(
                lambda v, m=max_len: v[:m] if isinstance(v, str) and len(v) > m else v
            )
        return staged[cols + ["snapshot_date"]]

    def _bulk_insert_socios_bcp(self, df: pd.DataFrame, snapshot_date: date) -> int:
        staged = self._prepare_socios_bcp_df(df, snapshot_date)
        if staged.empty:
            return 0

        limits = {
            "cnpj_basico": 8,
            "identificador_socio": 1,
            "nome_socio_razao_social": 150,
            "cnpj_cpf_socio": 14,
            "qualificacao_socio": 2,
            "data_entrada_sociedade": 10,
            "pais": 3,
            "representante_legal": 11,
            "nome_representante": 60,
            "qualificacao_representante_legal": 2,
            "faixa_etaria": 1,
        }
        stage_cols = list(limits) + ["snapshot_date"]
        table_name = self._stage_table_name("socios")
        columns = [(col, limits[col]) for col in limits] + [("snapshot_date", 10)]
        source_projection = self._source_projection([name for name, _ in columns])
        insert_columns = ", ".join(stage_cols + ["data_carga"])
        select_columns = ", ".join([f"s.{col}" for col in stage_cols] + ["GETDATE()"])

        try:
            self._create_stage_table(table_name, columns)
            self._bcp_load_dataframe(staged, table_name)
            with self.connect() as conn:
                cur = conn.cursor()
                cur.execute(
                    f"CREATE CLUSTERED INDEX IX_{table_name}_cnpj ON "
                    f"{_BCP_STAGE_SCHEMA}.{table_name} (cnpj_basico)"
                )
                cur.execute(f"""
                    DELETE t
                    FROM {self.schema}.socios AS t
                    INNER JOIN (
                        SELECT DISTINCT NULLIF(cnpj_basico, '') AS cnpj_basico
                        FROM {_BCP_STAGE_SCHEMA}.{table_name}
                    ) AS s
                        ON s.cnpj_basico = t.cnpj_basico
                    WHERE t.snapshot_date = ?
                """, (snapshot_date,))
                cur.execute(f"""
                    INSERT INTO {self.schema}.socios ({insert_columns})
                    SELECT {select_columns}
                    FROM (
                        SELECT
                            {source_projection}
                        FROM {_BCP_STAGE_SCHEMA}.{table_name}
                    ) AS s
                """)
                cur.execute(f"DROP TABLE IF EXISTS {_BCP_STAGE_SCHEMA}.{table_name}")
                return len(staged)
        finally:
            self._drop_stage_table(table_name)

    # ------------------------------------------------------------------
    # Bulk MERGE — estabelecimentos
    # ------------------------------------------------------------------

    def bulk_upsert_estabelecimentos(self, df: pd.DataFrame, snapshot_date: date) -> Tuple[int, int]:
        """
        MERGE idempotente para cnpj.estabelecimentos.
        Retorna (inseridos, atualizados).
        """
        if df.empty:
            return 0, 0
        if self._use_bcp("estabelecimentos"):
            return self._bulk_upsert_estabelecimentos_bcp(df, snapshot_date)

        required = [
            "cnpj_basico", "cnpj_ordem", "cnpj_dv",
            "identificador_matriz_filial", "nome_fantasia", "situacao_cadastral",
            "data_situacao_cadastral", "motivo_situacao_cadastral",
            "nome_cidade_exterior", "pais", "data_inicio_atividade",
            "cnae_fiscal", "cnae_fiscal_secundaria", "tipo_logradouro",
            "logradouro", "numero", "complemento", "bairro", "cep",
            "uf", "municipio", "ddd_1", "telefone_1", "ddd_2",
            "telefone_2", "ddd_fax", "fax", "correio_eletronico",
            "situacao_especial", "data_situacao_especial",
        ]
        df = df[required].copy()

        # CNPJ completo (chave de negócio)
        df["cnpj_completo"] = (
            df["cnpj_basico"].astype(str).str.strip().str.zfill(8)
            + df["cnpj_ordem"].astype(str).str.strip().str.zfill(4)
            + df["cnpj_dv"].astype(str).str.strip().str.zfill(2)
        )
        df = df[df["cnpj_completo"].str.len() == 14]

        # Ordem final das colunas para INSERT (deve bater com o #tmp_estab)
        col_order = ["cnpj_completo"] + required + ["snapshot_date"]
        df["snapshot_date"] = str(snapshot_date)
        df = df[col_order]

        # pandas 3.0 com pyarrow devolve StringDtype cujos nulos são pd.NA (não None).
        # .astype(object) converte para dtype object Python nativo; o .where() converte
        # pd.NA → None usando a máscara calculada ANTES do astype (pd.notnull(df)).
        df = df.astype(object).where(pd.notnull(df), None)
        for col in df.columns:
            df[col] = df[col].apply(lambda v: str(v) if pd.notna(v) else None)

        # Converte strings de data inválidas ('' ou datas impossíveis como '0000-00-00')
        # para None. O MERGE faz conversão implícita VARCHAR→DATE para estas colunas;
        # strings vazias ou datas inválidas causam SQLSTATE 22007 no SQL Server.
        for _dcol in ("data_situacao_cadastral", "data_inicio_atividade", "data_situacao_especial"):
            if _dcol in df.columns:
                def _clean_date(v, _d=_dcol):  # noqa: E306
                    if not isinstance(v, str) or len(v) != 10:
                        return None
                    try:
                        date.fromisoformat(v)
                        return v
                    except ValueError:
                        return None
                df[_dcol] = df[_dcol].apply(_clean_date)

        # Trunca colunas ao tamanho declarado em setinputsizes/tmp_estab.
        # ODBC Driver 18 levanta 22003 quando um valor excede o tamanho declarado.
        _ESTAB_MAXLEN = {
            "cnpj_basico":                8,
            "cnpj_ordem":                 4,
            "cnpj_dv":                    2,
            "identificador_matriz_filial": 1,
            "nome_fantasia":             55,
            "situacao_cadastral":         2,
            "motivo_situacao_cadastral":  2,
            "nome_cidade_exterior":      55,
            "pais":                       3,
            "cnae_fiscal":               10,
            "cnae_fiscal_secundaria":  1000,
            "tipo_logradouro":           20,
            "logradouro":                60,
            "numero":                     6,
            "complemento":              156,
            "bairro":                    50,
            "cep":                        8,
            "uf":                         2,
            "municipio":                  4,
            "ddd_1":                      4,
            "telefone_1":                 9,
            "ddd_2":                      4,
            "telefone_2":                 9,
            "ddd_fax":                    4,
            "fax":                        9,
            "correio_eletronico":       115,
            "situacao_especial":         23,
        }
        for _col, _max in _ESTAB_MAXLEN.items():
            if _col in df.columns:
                df[_col] = df[_col].apply(
                    lambda v, m=_max: v[:m] if isinstance(v, str) and len(v) > m else v
                )

        data = CNPJDatabase._df_to_data(df)
        if not data:
            return 0, 0

        target = f"{self.schema}.estabelecimentos"
        try:
            with self.connect() as conn:
                cur = conn.cursor()
                cur.fast_executemany = "ODBC Driver" in self.driver

                # Tamanhos explícitos evitam que fast_executemany infira o buffer
                # a partir de uma amostra e falhe com "right truncation" ao encontrar
                # valores maiores do que o máximo amostrado.
                _input_sizes = [
                    (pyodbc.SQL_VARCHAR,   14, 0),  # cnpj_completo
                    (pyodbc.SQL_VARCHAR,    8, 0),  # cnpj_basico
                    (pyodbc.SQL_VARCHAR,    4, 0),  # cnpj_ordem
                    (pyodbc.SQL_VARCHAR,    2, 0),  # cnpj_dv
                    (pyodbc.SQL_VARCHAR,    1, 0),  # identificador_matriz_filial
                    (pyodbc.SQL_VARCHAR,   55, 0),  # nome_fantasia
                    (pyodbc.SQL_VARCHAR,    2, 0),  # situacao_cadastral
                    (pyodbc.SQL_VARCHAR,   10, 0),  # data_situacao_cadastral
                    (pyodbc.SQL_VARCHAR,    2, 0),  # motivo_situacao_cadastral
                    (pyodbc.SQL_VARCHAR,   55, 0),  # nome_cidade_exterior
                    (pyodbc.SQL_VARCHAR,    3, 0),  # pais
                    (pyodbc.SQL_VARCHAR,   10, 0),  # data_inicio_atividade
                    (pyodbc.SQL_VARCHAR,   10, 0),  # cnae_fiscal
                    (pyodbc.SQL_VARCHAR, 1000, 0),  # cnae_fiscal_secundaria
                    (pyodbc.SQL_VARCHAR,   20, 0),  # tipo_logradouro
                    (pyodbc.SQL_VARCHAR,   60, 0),  # logradouro
                    (pyodbc.SQL_VARCHAR,    6, 0),  # numero
                    (pyodbc.SQL_VARCHAR,  156, 0),  # complemento
                    (pyodbc.SQL_VARCHAR,   50, 0),  # bairro
                    (pyodbc.SQL_VARCHAR,    8, 0),  # cep
                    (pyodbc.SQL_VARCHAR,    2, 0),  # uf
                    (pyodbc.SQL_VARCHAR,    4, 0),  # municipio
                    (pyodbc.SQL_VARCHAR,    4, 0),  # ddd_1
                    (pyodbc.SQL_VARCHAR,    9, 0),  # telefone_1
                    (pyodbc.SQL_VARCHAR,    4, 0),  # ddd_2
                    (pyodbc.SQL_VARCHAR,    9, 0),  # telefone_2
                    (pyodbc.SQL_VARCHAR,    4, 0),  # ddd_fax
                    (pyodbc.SQL_VARCHAR,    9, 0),  # fax
                    (pyodbc.SQL_VARCHAR,  115, 0),  # correio_eletronico
                    (pyodbc.SQL_VARCHAR,   23, 0),  # situacao_especial
                    (pyodbc.SQL_VARCHAR,   10, 0),  # data_situacao_especial
                    (pyodbc.SQL_VARCHAR,   10, 0),  # snapshot_date
                ]
                cur.execute("""
                    CREATE TABLE #tmp_estab (
                        cnpj_completo               VARCHAR(14),
                        cnpj_basico                 VARCHAR(8),
                        cnpj_ordem                  VARCHAR(4),
                        cnpj_dv                     VARCHAR(2),
                        identificador_matriz_filial VARCHAR(1),
                        nome_fantasia               VARCHAR(55),
                        situacao_cadastral          VARCHAR(2),
                        data_situacao_cadastral     VARCHAR(10),
                        motivo_situacao_cadastral   VARCHAR(2),
                        nome_cidade_exterior        VARCHAR(55),
                        pais                        VARCHAR(3),
                        data_inicio_atividade       VARCHAR(10),
                        cnae_fiscal                 VARCHAR(10),
                        cnae_fiscal_secundaria      VARCHAR(1000),
                        tipo_logradouro             VARCHAR(20),
                        logradouro                  VARCHAR(60),
                        numero                      VARCHAR(6),
                        complemento                 VARCHAR(156),
                        bairro                      VARCHAR(50),
                        cep                         VARCHAR(8),
                        uf                          VARCHAR(2),
                        municipio                   VARCHAR(4),
                        ddd_1                       VARCHAR(4),
                        telefone_1                  VARCHAR(9),
                        ddd_2                       VARCHAR(4),
                        telefone_2                  VARCHAR(9),
                        ddd_fax                     VARCHAR(4),
                        fax                         VARCHAR(9),
                        correio_eletronico          VARCHAR(115),
                        situacao_especial           VARCHAR(23),
                        data_situacao_especial      VARCHAR(10),
                        snapshot_date               VARCHAR(10)
                    )
                """)

                placeholders = ",".join(["?"] * len(col_order))
                self._chunked_executemany(
                    cur, f"INSERT INTO #tmp_estab VALUES ({placeholders})", data,
                    input_sizes=_input_sizes,
                )

                # Índice clusterizado ANTES do MERGE
                cur.execute("CREATE CLUSTERED INDEX IX_tmp_estab ON #tmp_estab (cnpj_completo)")

                cur.execute(f"""
                    MERGE {target} AS t
                    USING #tmp_estab AS s ON t.cnpj_completo = s.cnpj_completo
                    WHEN MATCHED AND (
                        ISNULL(t.situacao_cadastral,'')        <> ISNULL(s.situacao_cadastral,'') OR
                        ISNULL(CAST(t.data_situacao_cadastral AS VARCHAR),'')
                            <> ISNULL(CAST(s.data_situacao_cadastral AS VARCHAR),'') OR
                        ISNULL(t.nome_fantasia,'')             <> ISNULL(s.nome_fantasia,'') OR
                        ISNULL(t.cnae_fiscal,'')               <> ISNULL(s.cnae_fiscal,'') OR
                        ISNULL(t.motivo_situacao_cadastral,'') <> ISNULL(s.motivo_situacao_cadastral,'') OR
                        ISNULL(t.logradouro,'')                <> ISNULL(s.logradouro,'') OR
                        ISNULL(t.correio_eletronico,'')        <> ISNULL(s.correio_eletronico,'')
                    ) THEN
                        UPDATE SET
                            cnpj_basico                 = s.cnpj_basico,
                            cnpj_ordem                  = s.cnpj_ordem,
                            cnpj_dv                     = s.cnpj_dv,
                            identificador_matriz_filial = s.identificador_matriz_filial,
                            nome_fantasia               = s.nome_fantasia,
                            situacao_cadastral          = s.situacao_cadastral,
                            data_situacao_cadastral     = s.data_situacao_cadastral,
                            motivo_situacao_cadastral   = s.motivo_situacao_cadastral,
                            nome_cidade_exterior        = s.nome_cidade_exterior,
                            pais                        = s.pais,
                            data_inicio_atividade       = s.data_inicio_atividade,
                            cnae_fiscal                 = s.cnae_fiscal,
                            cnae_fiscal_secundaria      = s.cnae_fiscal_secundaria,
                            tipo_logradouro             = s.tipo_logradouro,
                            logradouro                  = s.logradouro,
                            numero                      = s.numero,
                            complemento                 = s.complemento,
                            bairro                      = s.bairro,
                            cep                         = s.cep,
                            uf                          = s.uf,
                            municipio                   = s.municipio,
                            ddd_1                       = s.ddd_1,
                            telefone_1                  = s.telefone_1,
                            ddd_2                       = s.ddd_2,
                            telefone_2                  = s.telefone_2,
                            ddd_fax                     = s.ddd_fax,
                            fax                         = s.fax,
                            correio_eletronico          = s.correio_eletronico,
                            situacao_especial           = s.situacao_especial,
                            data_situacao_especial      = s.data_situacao_especial,
                            snapshot_date               = s.snapshot_date,
                            data_carga                  = GETDATE()
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT (
                            cnpj_completo, cnpj_basico, cnpj_ordem, cnpj_dv,
                            identificador_matriz_filial, nome_fantasia, situacao_cadastral,
                            data_situacao_cadastral, motivo_situacao_cadastral,
                            nome_cidade_exterior, pais, data_inicio_atividade,
                            cnae_fiscal, cnae_fiscal_secundaria, tipo_logradouro,
                            logradouro, numero, complemento, bairro, cep, uf, municipio,
                            ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax, fax,
                            correio_eletronico, situacao_especial, data_situacao_especial,
                            snapshot_date, data_carga
                        )
                        VALUES (
                            s.cnpj_completo, s.cnpj_basico, s.cnpj_ordem, s.cnpj_dv,
                            s.identificador_matriz_filial, s.nome_fantasia, s.situacao_cadastral,
                            s.data_situacao_cadastral, s.motivo_situacao_cadastral,
                            s.nome_cidade_exterior, s.pais, s.data_inicio_atividade,
                            s.cnae_fiscal, s.cnae_fiscal_secundaria, s.tipo_logradouro,
                            s.logradouro, s.numero, s.complemento, s.bairro, s.cep,
                            s.uf, s.municipio,
                            s.ddd_1, s.telefone_1, s.ddd_2, s.telefone_2,
                            s.ddd_fax, s.fax,
                            s.correio_eletronico, s.situacao_especial, s.data_situacao_especial,
                            s.snapshot_date, GETDATE()
                        )
                    OUTPUT $action;
                """)

                actions = [r[0] for r in cur.fetchall()]
                inserted = actions.count("INSERT")
                updated = actions.count("UPDATE")
                cur.execute("DROP TABLE IF EXISTS #tmp_estab")
                return inserted, updated
        except Exception as e:
            logger.error("Erro no bulk_upsert_estabelecimentos: {}", e)
            raise

    # ------------------------------------------------------------------
    # Bulk MERGE — simples
    # ------------------------------------------------------------------

    def bulk_upsert_simples(self, df: pd.DataFrame, snapshot_date: date) -> Tuple[int, int]:
        """MERGE idempotente para cnpj.simples. Retorna (inseridos, atualizados)."""
        if df.empty:
            return 0, 0
        if self._use_bcp("simples"):
            return self._bulk_upsert_simples_bcp(df, snapshot_date)

        cols = [
            "cnpj_basico", "opcao_pelo_simples",
            "data_opcao_simples", "data_exclusao_simples",
            "opcao_mei", "data_opcao_mei", "data_exclusao_mei",
        ]
        df = df[cols].copy()
        df["cnpj_basico"] = df["cnpj_basico"].astype(str).str.strip().str.zfill(8)
        df = df[df["cnpj_basico"].notna() & (df["cnpj_basico"] != "") & (df["cnpj_basico"] != "nan")]
        df = df.astype(object).where(pd.notnull(df), None)

        # Sanitiza datas: mantém apenas strings YYYY-MM-DD que representem datas válidas.
        # "0000-00-00" passa o teste de formato mas SQL Server rejeita a conversão para DATE.
        def _valid_date_str(v) -> bool:
            if not isinstance(v, str) or len(v) != 10 or v[4] != "-":
                return False
            try:
                date.fromisoformat(v)
                return True
            except ValueError:
                return False

        date_cols = ["data_opcao_simples", "data_exclusao_simples", "data_opcao_mei", "data_exclusao_mei"]
        for col in date_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda v: v if _valid_date_str(v) else None)

        df["snapshot_date"] = str(snapshot_date)

        # Trunca ao tamanho declarado em setinputsizes para evitar SQLSTATE 22003.
        _SIMPLES_MAXLEN = {
            "cnpj_basico": 8, "opcao_pelo_simples": 1,
            "data_opcao_simples": 10, "data_exclusao_simples": 10,
            "opcao_mei": 1, "data_opcao_mei": 10, "data_exclusao_mei": 10,
        }
        for _col, _max in _SIMPLES_MAXLEN.items():
            if _col in df.columns:
                df[_col] = df[_col].apply(
                    lambda v, m=_max: v[:m] if isinstance(v, str) and len(v) > m else v
                )

        data = CNPJDatabase._df_to_data(df[cols + ["snapshot_date"]])
        if not data:
            return 0, 0

        target = f"{self.schema}.simples"
        try:
            with self.connect() as conn:
                cur = conn.cursor()
                cur.fast_executemany = "ODBC Driver" in self.driver

                cur.execute("""
                    CREATE TABLE #tmp_simples (
                        cnpj_basico           VARCHAR(8),
                        opcao_pelo_simples    VARCHAR(1),
                        data_opcao_simples    VARCHAR(10),
                        data_exclusao_simples VARCHAR(10),
                        opcao_mei             VARCHAR(1),
                        data_opcao_mei        VARCHAR(10),
                        data_exclusao_mei     VARCHAR(10),
                        snapshot_date         VARCHAR(10)
                    )
                """)
                # setinputsizes APÓS o CREATE TABLE: pyodbc 5.x descarta os tamanhos
                # após qualquer execute(), portanto deve ser definido imediatamente antes
                # do executemany. É passado como input_sizes para ser reaplicado a cada
                # lote de _chunked_executemany.
                _simples_sizes = [
                    (pyodbc.SQL_VARCHAR,  8, 0),  # cnpj_basico
                    (pyodbc.SQL_VARCHAR,  1, 0),  # opcao_pelo_simples
                    (pyodbc.SQL_VARCHAR, 10, 0),  # data_opcao_simples
                    (pyodbc.SQL_VARCHAR, 10, 0),  # data_exclusao_simples
                    (pyodbc.SQL_VARCHAR,  1, 0),  # opcao_mei
                    (pyodbc.SQL_VARCHAR, 10, 0),  # data_opcao_mei
                    (pyodbc.SQL_VARCHAR, 10, 0),  # data_exclusao_mei
                    (pyodbc.SQL_VARCHAR, 10, 0),  # snapshot_date
                ]
                self._chunked_executemany(
                    cur, "INSERT INTO #tmp_simples VALUES (?,?,?,?,?,?,?,?)", data,
                    input_sizes=_simples_sizes,
                )

                # Índice clusterizado ANTES do MERGE
                cur.execute("CREATE CLUSTERED INDEX IX_tmp_simples ON #tmp_simples (cnpj_basico)")

                cur.execute(f"""
                    MERGE {target} AS t
                    USING #tmp_simples AS s ON t.cnpj_basico = s.cnpj_basico
                    WHEN MATCHED AND (
                        ISNULL(t.opcao_pelo_simples,'') <> ISNULL(s.opcao_pelo_simples,'') OR
                        ISNULL(t.opcao_mei,'')          <> ISNULL(s.opcao_mei,'')
                    ) THEN
                        UPDATE SET
                            opcao_pelo_simples    = s.opcao_pelo_simples,
                            data_opcao_simples    = s.data_opcao_simples,
                            data_exclusao_simples = s.data_exclusao_simples,
                            opcao_mei             = s.opcao_mei,
                            data_opcao_mei        = s.data_opcao_mei,
                            data_exclusao_mei     = s.data_exclusao_mei,
                            snapshot_date         = s.snapshot_date,
                            data_carga            = GETDATE()
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT (cnpj_basico, opcao_pelo_simples,
                                data_opcao_simples, data_exclusao_simples,
                                opcao_mei, data_opcao_mei, data_exclusao_mei,
                                snapshot_date, data_carga)
                        VALUES (s.cnpj_basico, s.opcao_pelo_simples,
                                s.data_opcao_simples, s.data_exclusao_simples,
                                s.opcao_mei, s.data_opcao_mei, s.data_exclusao_mei,
                                s.snapshot_date, GETDATE())
                    OUTPUT $action;
                """)

                actions = [r[0] for r in cur.fetchall()]
                inserted = actions.count("INSERT")
                updated = actions.count("UPDATE")
                cur.execute("DROP TABLE IF EXISTS #tmp_simples")
                return inserted, updated
        except Exception as e:
            logger.error("Erro no bulk_upsert_simples: {}", e)
            raise

    # ------------------------------------------------------------------
    # Bulk INSERT — socios (sem chave natural única → DELETE + INSERT)
    # ------------------------------------------------------------------

    def bulk_insert_socios(self, df: pd.DataFrame, snapshot_date: date) -> int:
        """
        Deleta socios do snapshot atual para os cnpj_basico do lote,
        depois insere os novos. Idempotente: re-rodar o mesmo arquivo
        não cria duplicatas porque deleta antes de inserir.
        """
        if df.empty:
            return 0
        if self._use_bcp("socios"):
            return self._bulk_insert_socios_bcp(df, snapshot_date)

        cols = [
            "cnpj_basico", "identificador_socio", "nome_socio_razao_social",
            "cnpj_cpf_socio", "qualificacao_socio", "data_entrada_sociedade",
            "pais", "representante_legal", "nome_representante",
            "qualificacao_representante_legal", "faixa_etaria",
        ]
        df = df[cols].copy()
        df["cnpj_basico"] = df["cnpj_basico"].astype(str).str.strip().str.zfill(8)
        df = df[df["cnpj_basico"].notna() & (df["cnpj_basico"] != "") & (df["cnpj_basico"] != "nan")]
        df = df.astype(object).where(pd.notnull(df), None)
        # Garante que todas as colunas string são Python str nativo.
        # pandas 3.0 + pyarrow devolve StringDtype com pd.NA como nulo — pd.notna(v)
        # trata pd.NA, numpy.nan e None de forma uniforme.
        _str_cols = [c for c in cols if c != "data_entrada_sociedade"]
        for _col in _str_cols:
            df[_col] = df[_col].apply(lambda v: str(v) if pd.notna(v) else None)

        # Converter data_entrada_sociedade para datetime.date (ou None).
        # Usa try/except para descartar datas inválidas como "0000-00-00".
        def _safe_date(v):
            if not isinstance(v, str) or len(v) != 10:
                return None
            try:
                return date.fromisoformat(v)
            except ValueError:
                return None

        df["data_entrada_sociedade"] = df["data_entrada_sociedade"].apply(_safe_date)
        # snapshot_date como objeto date (não string) para colunas DATE do SQL Server
        df["snapshot_date"] = snapshot_date

        if df.empty:
            return 0

        # Trunca ao tamanho declarado em setinputsizes para evitar SQLSTATE 22003.
        _SOCIOS_MAXLEN = {
            "cnpj_basico":                    8,
            "identificador_socio":            1,
            "nome_socio_razao_social":       150,
            "cnpj_cpf_socio":                14,
            "qualificacao_socio":             2,
            "pais":                           3,
            "representante_legal":           11,
            "nome_representante":            60,
            "qualificacao_representante_legal": 2,
            "faixa_etaria":                   1,
        }
        for _col, _max in _SOCIOS_MAXLEN.items():
            if _col in df.columns:
                df[_col] = df[_col].apply(
                    lambda v, m=_max: v[:m] if isinstance(v, str) and len(v) > m else v
                )

        # CNPJ basicos únicos neste lote para o DELETE seletivo
        cnpj_list = df["cnpj_basico"].unique().tolist()
        data = CNPJDatabase._df_to_data(df[cols + ["snapshot_date"]])

        target = f"{self.schema}.socios"
        # Tentativas com backoff para erros de conectividade (08S01, 08001).
        # A operação é idempotente: DELETE antes do INSERT garante que retentar
        # o mesmo lote não gera duplicatas.
        for attempt in Retrying(
            retry=retry_if_exception_type(pyodbc.OperationalError),
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=30, max=300),
            reraise=True,
        ):
            with attempt:
                try:
                    with self.connect() as conn:
                        cur = conn.cursor()
                        cur.fast_executemany = "ODBC Driver" in self.driver

                        # Deleta apenas os socios deste snapshot para os CNPJs do lote
                        # em batches de 1000 para não ultrapassar o limite de 2100 parâmetros do ODBC
                        _DELETE_BATCH = 1000
                        for _i in range(0, len(cnpj_list), _DELETE_BATCH):
                            _batch = cnpj_list[_i:_i + _DELETE_BATCH]
                            _ph = ",".join(["?"] * len(_batch))
                            cur.execute(
                                f"DELETE FROM {target} "
                                f"WHERE snapshot_date = ? AND cnpj_basico IN ({_ph})",
                                (snapshot_date, *_batch),
                            )

                        placeholders_row = ",".join(["?"] * (len(cols) + 1))
                        # Tipos explícitos para fast_executemany: sem setinputsizes, quando
                        # a primeira linha tem None em data_entrada_sociedade, pyodbc infere
                        # SQL_NULL para toda a coluna e ignora valores date nas linhas seguintes,
                        # causando SQLSTATE 22003. SQL_TYPE_DATE garante o tipo correto.
                        _socios_sizes = [
                            (pyodbc.SQL_VARCHAR,    8, 0),  # cnpj_basico
                            (pyodbc.SQL_VARCHAR,    1, 0),  # identificador_socio
                            (pyodbc.SQL_VARCHAR,  150, 0),  # nome_socio_razao_social
                            (pyodbc.SQL_VARCHAR,   14, 0),  # cnpj_cpf_socio
                            (pyodbc.SQL_VARCHAR,    2, 0),  # qualificacao_socio
                            (pyodbc.SQL_TYPE_DATE,  0, 0),  # data_entrada_sociedade
                            (pyodbc.SQL_VARCHAR,    3, 0),  # pais
                            (pyodbc.SQL_VARCHAR,   11, 0),  # representante_legal
                            (pyodbc.SQL_VARCHAR,   60, 0),  # nome_representante
                            (pyodbc.SQL_VARCHAR,    2, 0),  # qualificacao_representante_legal
                            (pyodbc.SQL_VARCHAR,    1, 0),  # faixa_etaria
                            (pyodbc.SQL_TYPE_DATE,  0, 0),  # snapshot_date
                        ]
                        self._chunked_executemany(
                            cur,
                            f"INSERT INTO {target} "
                            f"({', '.join(cols)}, snapshot_date, data_carga) "
                            f"VALUES ({placeholders_row}, GETDATE())",
                            data,
                            input_sizes=_socios_sizes,
                        )
                        return len(data)
                except pyodbc.OperationalError as e:
                    logger.warning("Erro de conexão em bulk_insert_socios (vai tentar novamente): {}", e)
                    raise
                except Exception as e:
                    logger.error("Erro no bulk_insert_socios: {}", e)
                    raise

    # ------------------------------------------------------------------
    # Informações do banco (diagnóstico)
    # ------------------------------------------------------------------

    def get_database_info(self) -> Dict[str, Any]:
        try:
            rows = self.execute_query(
                "SELECT DB_NAME(), @@VERSION, @@SERVERNAME"
            )
            size = self.execute_query(
                "SELECT SUM(size * 8 / 1024) FROM sys.master_files WHERE database_id = DB_ID()"
            )
            tables = self.execute_query(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ?",
                (self.schema,),
            )
            r = rows[0]
            return {
                "database_name": r[0],
                "sql_version": r[1],
                "server_name": r[2],
                "size_mb": size[0][0] if size else 0,
                "table_count": tables[0][0] if tables else 0,
            }
        except Exception as e:
            logger.error("Erro ao obter informações do banco: {}", e)
            return {}
