"""
Módulo de conexão com PostgreSQL para persistência dos dados CNPJ.

Fornece:
  - Conexão com PostgreSQL via psycopg2
  - Bulk load via COPY FROM STDIN (texto, separado por tab)
  - Upserts via INSERT ... ON CONFLICT ... DO UPDATE ... RETURNING
  - Controle idempotente de sincronização via controle_sincronizacao
"""

from __future__ import annotations

import io
import os
import re
import uuid
from contextlib import contextmanager, nullcontext
from datetime import date
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

import pandas as pd
import psycopg2
import psycopg2.extensions
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.config import DB_CHUNK_ROWS
from src.logger_enhanced import logger, structured_logger

_CHUNK_SIZE = DB_CHUNK_ROWS

# Strings que representam valores nulos vindos do pandas/parquet
_NULL_STRS: frozenset[str] = frozenset({
    "nan", "NaN", "NAN", "None", "NONE", "none",
    "NULL", "null", "Null", "<NA>", "na", "NA", "N/A", "n/a",
})


# ---------------------------------------------------------------------------
# Conexão base
# ---------------------------------------------------------------------------

class PostgreSQLConnection:
    """Gerencia conexões com PostgreSQL."""

    def __init__(
        self,
        host: str = "localhost",
        port: Optional[int] = None,
        database: str = "postgres",
        username: Optional[str] = None,
        password: Optional[str] = None,
        schema: str = "cnpj",
        database_url: Optional[str] = None,
    ):
        self.host = host
        self.port = port or int(os.getenv("DB_PORT", "5432"))
        self.database = database
        self.schema = schema
        self.username = username or os.getenv("DB_USERNAME") or os.getenv("POSTGRES_USER")
        self.password = password or os.getenv("DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD")
        self.database_url = database_url or os.getenv("DATABASE_URL")

    def _get_dsn(self) -> str:
        if self.database_url:
            return self.database_url
        dsn = f"host={self.host} port={self.port} dbname={self.database}"
        if self.username:
            dsn += f" user={self.username}"
        if self.password:
            dsn += f" password={self.password}"
        return dsn

    @contextmanager
    def connect(self, autocommit: bool = False) -> Generator[psycopg2.extensions.connection, None, None]:
        conn = None
        try:
            conn = psycopg2.connect(self._get_dsn())
            conn.autocommit = autocommit
            yield conn
            if not autocommit:
                conn.commit()
        except Exception:
            if conn and not autocommit:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def test_connection(self) -> bool:
        try:
            with self.connect() as conn:
                conn.cursor().execute("SELECT 1")
            return True
        except Exception as e:
            logger.error("Falha no teste de conexão: {}", e)
            return False

    def execute_query(self, query: str, params: tuple = ()) -> List[Tuple]:
        query = query.replace("?", "%s")
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(query, params or None)
            return cur.fetchall()

    def execute_non_query(self, query: str, params: tuple = ()) -> int:
        query = query.replace("?", "%s")
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(query, params or None)
            return cur.rowcount


# ---------------------------------------------------------------------------
# Classe especializada CNPJ
# ---------------------------------------------------------------------------

class CNPJDatabase(PostgreSQLConnection):
    """Operações específicas do schema CNPJ — PostgreSQL."""

    def __init__(
        self,
        database: str = "postgres",
        server: Optional[str] = None,
        host: str = "localhost",
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs,
    ):
        # 'server' é aceito como alias de 'host' para compatibilidade com main.py
        super().__init__(
            host=server or host,
            port=port,
            database=database,
            username=username,
            password=password,
            **kwargs,
        )
        self.schema = "cnpj"
        logger.info("Backend: PostgreSQL {}:{}/{}", self.host, self.port, self.database)

    # ------------------------------------------------------------------
    # Helpers de COPY
    # ------------------------------------------------------------------

    @staticmethod
    def _df_to_copy_buffer(df: pd.DataFrame) -> io.StringIO:
        """Converte DataFrame para buffer tab-delimitado apto ao COPY FROM STDIN (FORMAT TEXT)."""
        export = df.copy().astype(object).where(pd.notnull(df), None)

        buf = io.StringIO()
        for row in export.itertuples(index=False, name=None):
            parts: list[str] = []
            for v in row:
                if v is None:
                    parts.append("\\N")
                elif isinstance(v, float) and v != v:  # float NaN: NaN != NaN
                    parts.append("\\N")
                elif isinstance(v, str):
                    if not v or v in _NULL_STRS:
                        parts.append("\\N")
                    else:
                        parts.append(
                            v.replace("\x00", "")
                             .replace("\\", "\\\\")
                             .replace("\t", " ")
                             .replace("\n", " ")
                             .replace("\r", " ")
                        )
                else:
                    parts.append(str(v))
            buf.write("\t".join(parts) + "\n")
        buf.seek(0)
        return buf

    def _copy_df_to_temp(
        self,
        cur: psycopg2.extensions.cursor,
        df: pd.DataFrame,
        temp_name: str,
        ddl: str,
    ) -> None:
        """Cria tabela temporária e carrega DataFrame via COPY FROM STDIN."""
        cur.execute(f"CREATE TEMP TABLE {temp_name} ({ddl}) ON COMMIT DROP")
        buf = self._df_to_copy_buffer(df)
        cur.copy_expert(f"COPY {temp_name} FROM STDIN", buf)

    @contextmanager
    def _cursor_scope(
        self,
        conn: Optional[psycopg2.extensions.connection] = None,
    ) -> Generator[psycopg2.extensions.cursor, None, None]:
        """
        Reuse an existing connection when provided, otherwise open a short-lived one.

        The caller owns transaction boundaries when passing ``conn`` explicitly.
        """
        conn_cm = nullcontext(conn) if conn is not None else self.connect()
        with conn_cm as active_conn:
            cur = active_conn.cursor()
            try:
                yield cur
            finally:
                cur.close()

    @staticmethod
    def _valid_iso_date_str(value: Any) -> Optional[str]:
        if not isinstance(value, str) or len(value) != 10:
            return None
        try:
            date.fromisoformat(value)
            return value
        except ValueError:
            return None

    # ------------------------------------------------------------------
    # Inicialização
    # ------------------------------------------------------------------

    def create_database_if_not_exists(self) -> bool:
        """No PostgreSQL, o banco já deve existir. Apenas verifica a conexão."""
        try:
            with self.connect() as conn:
                cur = conn.cursor()
                cur.execute("SELECT current_database()")
                db = cur.fetchone()[0]
                logger.debug("Banco '{}' acessível", db)
            return True
        except Exception as e:
            logger.error("Erro ao verificar banco: {}", e)
            return False

    def execute_schema_script(self, script_path: Path) -> bool:
        """Executa script SQL PostgreSQL (sem blocos GO)."""
        try:
            script = script_path.read_text(encoding="utf-8")
            # Remove comentários de linha antes de dividir por ;
            lines = []
            for line in script.splitlines():
                stripped = line.split("--")[0].rstrip()
                lines.append(stripped)
            clean = "\n".join(lines)
            statements = [s.strip() for s in re.split(r";\s*\n", clean) if s.strip()]

            with self.connect(autocommit=True) as conn:
                cur = conn.cursor()
                for stmt in statements:
                    if stmt:
                        try:
                            cur.execute(stmt)
                        except Exception as e:
                            logger.error("Erro no bloco do script: {}", e)
                            logger.debug("Comando: {}", stmt[:300])
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
        try:
            rows = self.execute_query(
                f"SELECT 1 FROM {self.schema}.controle_sincronizacao "
                "WHERE snapshot_date = %s AND status = 'SUCESSO'",
                (str(snapshot_date),),
            )
            return bool(rows)
        except Exception as e:
            logger.error("Erro ao verificar snapshot: {}", e)
            return False

    def is_snapshot_running(self, snapshot_date: date) -> bool:
        try:
            rows = self.execute_query(
                f"SELECT 1 FROM {self.schema}.controle_sincronizacao "
                "WHERE snapshot_date = %s AND status = 'EM_EXECUCAO'",
                (str(snapshot_date),),
            )
            return bool(rows)
        except Exception as e:
            logger.error("Erro ao verificar execução em andamento: {}", e)
            return False

    def start_sync_session(self, snapshot_date: date, force: bool = False) -> Optional[int]:
        snap_str = str(snapshot_date)
        try:
            with self.connect() as conn:
                cur = conn.cursor()

                if force:
                    cur.execute(
                        f"UPDATE {self.schema}.controle_sincronizacao "
                        "SET status = 'FALHA', data_fim_execucao = NOW(), "
                        "erro_mensagem = 'Interrompida por --force' "
                        "WHERE snapshot_date = %s AND status = 'EM_EXECUCAO'",
                        (snap_str,),
                    )

                cur.execute(
                    f"INSERT INTO {self.schema}.controle_sincronizacao "
                    "(snapshot_date, status, data_inicio_execucao) "
                    "VALUES (%s, 'EM_EXECUCAO', NOW()) RETURNING id_execucao",
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
            sets = ["status = %s"]
            params: List[Any] = [status]

            if total_files is not None:
                sets.append("total_arquivos = %s"); params.append(total_files)
            if processed_files is not None:
                sets.append("arquivos_processados = %s"); params.append(processed_files)
            if failed_files is not None:
                sets.append("arquivos_falha = %s"); params.append(failed_files)
            if total_records is not None:
                sets.append("total_registros = %s"); params.append(total_records)
            if status in ("SUCESSO", "FALHA", "CANCELADO"):
                sets.append("data_fim_execucao = NOW()")
                sets.append(
                    "duracao_segundos = "
                    "EXTRACT(EPOCH FROM (NOW() - data_inicio_execucao))::INT"
                )
            if error_message:
                sets.append("erro_mensagem = %s"); params.append(str(error_message)[:4000])

            params.append(exec_id)
            with self.connect() as conn:
                cur = conn.cursor()
                cur.execute(
                    f"UPDATE {self.schema}.controle_sincronizacao "
                    f"SET {', '.join(sets)} WHERE id_execucao = %s",
                    tuple(params),
                )
            logger.info("Sessão {} → {}", exec_id, status)
            return True
        except Exception as e:
            logger.error("Erro ao atualizar sessão: {}", e)
            return False

    def add_file_to_sync(
        self, exec_id: int, group: str, filename: str, status: str = "PENDENTE"
    ) -> Optional[int]:
        try:
            with self.connect() as conn:
                cur = conn.cursor()
                cur.execute(
                    f"INSERT INTO {self.schema}.controle_arquivos "
                    "(id_execucao, grupo_arquivo, nome_arquivo, status, data_inicio) "
                    "VALUES (%s, %s, %s, %s, NOW()) RETURNING id_arquivo",
                    (exec_id, group, filename, status),
                )
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
            sets = ["status = %s", "data_fim = NOW()"]
            params: List[Any] = [status]
            if total_records is not None:
                sets.append("total_registros = %s"); params.append(total_records)
            if invalid_records is not None:
                sets.append("registros_invalidos = %s"); params.append(invalid_records)
            if error_message:
                sets.append("erro_mensagem = %s"); params.append(str(error_message)[:2000])
            params.append(file_id)
            with self.connect() as conn:
                cur = conn.cursor()
                cur.execute(
                    f"UPDATE {self.schema}.controle_arquivos "
                    f"SET {', '.join(sets)} WHERE id_arquivo = %s",
                    tuple(params),
                )
            return True
        except Exception as e:
            logger.error("Erro ao atualizar arquivo {}: {}", file_id, e)
            return False

    def get_downloaded_files(self, snapshot_date: date) -> dict[str, str]:
        """Retorna {nome_arquivo: caminho_local} dos arquivos já baixados para este snapshot."""
        try:
            rows = self.execute_query(
                f"SELECT nome_arquivo, COALESCE(caminho_local, '') "
                f"FROM {self.schema}.controle_downloads "
                "WHERE snapshot_date = %s AND status = 'BAIXADO'",
                (str(snapshot_date),),
            )
            return {row[0]: row[1] for row in rows} if rows else {}
        except Exception as e:
            logger.error("Erro ao obter downloads registrados: {}", e)
            return {}

    def register_download(
        self,
        snapshot_date: date,
        filename: str,
        group: str,
        status: str,
        size_bytes: Optional[int] = None,
        local_path: Optional[str] = None,
        error: Optional[str] = None,
    ) -> bool:
        """Grava ou atualiza o status de download de um arquivo (upsert por snapshot+nome)."""
        try:
            with self.connect() as conn:
                cur = conn.cursor()
                cur.execute(
                    f"""
                    INSERT INTO {self.schema}.controle_downloads
                        (snapshot_date, nome_arquivo, grupo_arquivo, status,
                         tamanho_bytes, caminho_local, data_inicio, data_fim, erro_mensagem)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW(), %s)
                    ON CONFLICT (snapshot_date, nome_arquivo) DO UPDATE SET
                        status        = EXCLUDED.status,
                        tamanho_bytes = COALESCE(EXCLUDED.tamanho_bytes, controle_downloads.tamanho_bytes),
                        caminho_local = COALESCE(EXCLUDED.caminho_local, controle_downloads.caminho_local),
                        data_fim      = NOW(),
                        erro_mensagem = EXCLUDED.erro_mensagem
                    """,
                    (str(snapshot_date), filename, group, status, size_bytes, local_path,
                     str(error)[:2000] if error else None),
                )
            return True
        except Exception as e:
            logger.error("Erro ao registrar download de {}: {}", filename, e)
            return False

    def get_successfully_loaded_files(self, snapshot_date: date) -> dict:
        try:
            rows = self.execute_query(
                f"SELECT ca.nome_arquivo, COALESCE(ca.total_registros, 0) "
                f"FROM {self.schema}.controle_arquivos ca "
                f"JOIN {self.schema}.controle_sincronizacao cs ON ca.id_execucao = cs.id_execucao "
                "WHERE cs.snapshot_date = %s AND ca.status = 'SUCESSO'",
                (str(snapshot_date),),
            )
            result = {row[0]: row[1] for row in rows} if rows else {}
            logger.debug("Arquivos já carregados para {}: {}", snapshot_date, list(result.keys()))
            return result
        except Exception as e:
            logger.error("Erro ao verificar arquivos já carregados: {}", e)
            return {}

    # ------------------------------------------------------------------
    # Helpers de preparação de DataFrame
    # ------------------------------------------------------------------

    @staticmethod
    def _nullify_empty(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
        """Converte strings vazias e NaN para None nas colunas especificadas."""
        for col in cols:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda v: None if (v is None or (isinstance(v, float) and v != v)
                                       or v is pd.NA or v == "") else v
                )
        return df

    @staticmethod
    def _truncate_cols(df: pd.DataFrame, limits: dict) -> pd.DataFrame:
        for col, max_len in limits.items():
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda v, m=max_len: v[:m] if isinstance(v, str) and len(v) > m else v
                )
        return df

    # ------------------------------------------------------------------
    # Bulk upsert — tabelas de referência
    # ------------------------------------------------------------------

    def bulk_upsert_reference(
        self,
        table: str,
        df: pd.DataFrame,
        snapshot_date: date,
        conn: Optional[psycopg2.extensions.connection] = None,
    ) -> int:
        if df.empty:
            return 0

        staged = df[["codigo", "descricao"]].copy()
        staged["codigo"] = staged["codigo"].astype(str).str.strip()
        staged["descricao"] = staged["descricao"].fillna("").astype(str).str.strip()
        staged = staged[
            staged["codigo"].notna()
            & (staged["codigo"] != "")
            & (staged["codigo"] != "nan")
        ]

        dups = staged.duplicated(subset="codigo", keep=False).sum()
        if dups:
            logger.warning("{} linhas duplicadas em 'codigo' em {} — mantendo última ocorrência", dups, table)
            staged = staged.drop_duplicates(subset="codigo", keep="last").reset_index(drop=True)

        staged = staged.astype(object).where(pd.notnull(staged), None)
        staged = staged.replace(list(_NULL_STRS), None)
        staged["snapshot_date"] = str(snapshot_date)
        staged = staged[["codigo", "descricao", "snapshot_date"]]

        temp = f"tmp_ref_{uuid.uuid4().hex[:10]}"
        ddl = "codigo TEXT, descricao TEXT, snapshot_date TEXT"

        try:
            with self._cursor_scope(conn) as cur:
                self._copy_df_to_temp(cur, staged, temp, ddl)
                cur.execute(f"""
                    INSERT INTO {self.schema}.{table} (codigo, descricao, snapshot_date, data_carga)
                    SELECT
                        NULLIF(TRIM(codigo), '') AS codigo,
                        COALESCE(NULLIF(TRIM(descricao), ''), '') AS descricao,
                        snapshot_date::DATE,
                        NOW()
                    FROM {temp}
                    WHERE NULLIF(TRIM(codigo), '') IS NOT NULL
                    ON CONFLICT (codigo) DO UPDATE SET
                        descricao     = EXCLUDED.descricao,
                        snapshot_date = EXCLUDED.snapshot_date,
                        data_carga    = NOW()
                    RETURNING (xmax = 0)
                """)
                rows = cur.fetchall()
                affected = len(rows)
                logger.info("{}: {} registros mesclados", table, affected)
                return affected
        except Exception as e:
            logger.error("Erro no bulk_upsert_reference [{}]: {}", table, e)
            raise

    # ------------------------------------------------------------------
    # Bulk upsert — empresas
    # ------------------------------------------------------------------

    def bulk_upsert_empresas(
        self,
        df: pd.DataFrame,
        snapshot_date: date,
        conn: Optional[psycopg2.extensions.connection] = None,
    ) -> Tuple[int, int]:
        if df.empty:
            return 0, 0

        limits = {
            "cnpj_basico": 8, "razao_social": 150, "natureza_juridica": 4,
            "qualificacao_responsavel": 2, "porte_empresa": 2,
            "ente_federativo_responsavel": 50,
        }
        cols = [
            "cnpj_basico", "razao_social", "natureza_juridica",
            "qualificacao_responsavel", "capital_social",
            "porte_empresa", "ente_federativo_responsavel",
        ]
        staged = df[cols].copy()
        staged["cnpj_basico"] = staged["cnpj_basico"].astype(str).str.strip().str.zfill(8)
        staged = staged[
            staged["cnpj_basico"].notna()
            & (staged["cnpj_basico"] != "")
            & (staged["cnpj_basico"] != "nan")
        ]
        staged["capital_social"] = pd.to_numeric(staged["capital_social"], errors="coerce")
        staged = staged.astype(object).where(pd.notnull(staged), None)
        staged["capital_social"] = [float(v) if v is not None else None for v in staged["capital_social"]]
        for col, max_len in limits.items():
            if col in staged.columns:
                staged[col] = staged[col].apply(
                    lambda v, m=max_len: str(v)[:m] if v is not None else None
                )

        staged = staged.replace(list(_NULL_STRS), None)

        dups = staged.duplicated(subset="cnpj_basico", keep=False).sum()
        if dups:
            logger.warning("bulk_upsert_empresas: {} duplicatas em cnpj_basico — mantendo última", dups)
            staged = staged.drop_duplicates(subset="cnpj_basico", keep="last").reset_index(drop=True)

        staged["snapshot_date"] = str(snapshot_date)
        # Formata capital_social como string decimal para o COPY buffer
        staged["capital_social"] = staged["capital_social"].apply(
            lambda v: f"{v:.2f}" if v is not None else None
        )
        staged = staged[cols + ["snapshot_date"]]

        temp = f"tmp_emp_{uuid.uuid4().hex[:10]}"
        ddl = (
            "cnpj_basico TEXT, razao_social TEXT, natureza_juridica TEXT, "
            "qualificacao_responsavel TEXT, capital_social TEXT, "
            "porte_empresa TEXT, ente_federativo_responsavel TEXT, snapshot_date TEXT"
        )

        try:
            with self._cursor_scope(conn) as cur:
                self._copy_df_to_temp(cur, staged, temp, ddl)
                cur.execute(f"""
                    INSERT INTO {self.schema}.empresas (
                        cnpj_basico, razao_social, natureza_juridica,
                        qualificacao_responsavel, capital_social,
                        porte_empresa, ente_federativo_responsavel,
                        snapshot_date, data_carga
                    )
                    SELECT
                        NULLIF(LEFT(cnpj_basico, 8), ''),
                        NULLIF(LEFT(razao_social, 150), ''),
                        NULLIF(LEFT(natureza_juridica, 4), ''),
                        NULLIF(LEFT(qualificacao_responsavel, 2), ''),
                        capital_social::NUMERIC(18,2),
                        NULLIF(LEFT(porte_empresa, 2), ''),
                        NULLIF(LEFT(ente_federativo_responsavel, 50), ''),
                        snapshot_date::DATE,
                        NOW()
                    FROM {temp}
                    WHERE NULLIF(cnpj_basico, '') IS NOT NULL
                    ON CONFLICT (cnpj_basico) DO UPDATE SET
                        razao_social                = EXCLUDED.razao_social,
                        natureza_juridica           = EXCLUDED.natureza_juridica,
                        qualificacao_responsavel    = EXCLUDED.qualificacao_responsavel,
                        capital_social              = EXCLUDED.capital_social,
                        porte_empresa               = EXCLUDED.porte_empresa,
                        ente_federativo_responsavel = EXCLUDED.ente_federativo_responsavel,
                        snapshot_date               = EXCLUDED.snapshot_date,
                        data_carga                  = NOW()
                    RETURNING (xmax = 0)
                """)
                results = cur.fetchall()
                inserted = sum(1 for r in results if r[0])
                updated = sum(1 for r in results if not r[0])
                return inserted, updated
        except Exception as e:
            logger.error("Erro no bulk_upsert_empresas: {}", e)
            raise

    # ------------------------------------------------------------------
    # Bulk upsert — estabelecimentos
    # ------------------------------------------------------------------

    def _prepare_estabelecimentos_df(self, df: pd.DataFrame, snapshot_date: date) -> pd.DataFrame:
        limits = {
            "cnpj_basico": 8, "cnpj_ordem": 4, "cnpj_dv": 2,
            "identificador_matriz_filial": 1, "nome_fantasia": 55,
            "situacao_cadastral": 2, "data_situacao_cadastral": 10,
            "motivo_situacao_cadastral": 2, "nome_cidade_exterior": 55,
            "pais": 3, "data_inicio_atividade": 10, "cnae_fiscal": 10,
            "cnae_fiscal_secundaria": 1000, "tipo_logradouro": 20,
            "logradouro": 60, "numero": 6, "complemento": 156,
            "bairro": 50, "cep": 8, "uf": 2, "municipio": 4,
            "ddd_1": 4, "telefone_1": 9, "ddd_2": 4, "telefone_2": 9,
            "ddd_fax": 4, "fax": 9, "correio_eletronico": 115,
            "situacao_especial": 23, "data_situacao_especial": 10,
        }
        required = list(limits)
        staged = df[required].copy()
        staged["cnpj_completo"] = (
            staged["cnpj_basico"].astype(str).str.strip().str.zfill(8)
            + staged["cnpj_ordem"].astype(str).str.strip().str.zfill(4)
            + staged["cnpj_dv"].astype(str).str.strip().str.zfill(2)
        )
        staged = staged[staged["cnpj_completo"].str.len() == 14]
        staged = staged.astype(object).where(pd.notnull(staged), None)

        for col in staged.columns:
            staged[col] = staged[col].apply(
                lambda v: None if (not pd.notna(v) or (isinstance(v, str) and v in _NULL_STRS))
                else str(v)
            )

        for dcol in ("data_situacao_cadastral", "data_inicio_atividade", "data_situacao_especial"):
            staged[dcol] = staged[dcol].apply(self._valid_iso_date_str)

        staged = staged.replace(list(_NULL_STRS), None)
        staged = self._truncate_cols(staged, limits)
        staged["snapshot_date"] = str(snapshot_date)
        return staged[["cnpj_completo"] + required + ["snapshot_date"]]

    def bulk_upsert_estabelecimentos(
        self,
        df: pd.DataFrame,
        snapshot_date: date,
        conn: Optional[psycopg2.extensions.connection] = None,
    ) -> Tuple[int, int]:
        if df.empty:
            return 0, 0

        staged = self._prepare_estabelecimentos_df(df, snapshot_date)
        if staged.empty:
            return 0, 0

        temp = f"tmp_estab_{uuid.uuid4().hex[:10]}"
        ddl = (
            "cnpj_completo TEXT, cnpj_basico TEXT, cnpj_ordem TEXT, cnpj_dv TEXT, "
            "identificador_matriz_filial TEXT, nome_fantasia TEXT, situacao_cadastral TEXT, "
            "data_situacao_cadastral TEXT, motivo_situacao_cadastral TEXT, "
            "nome_cidade_exterior TEXT, pais TEXT, data_inicio_atividade TEXT, "
            "cnae_fiscal TEXT, cnae_fiscal_secundaria TEXT, tipo_logradouro TEXT, "
            "logradouro TEXT, numero TEXT, complemento TEXT, bairro TEXT, cep TEXT, "
            "uf TEXT, municipio TEXT, ddd_1 TEXT, telefone_1 TEXT, ddd_2 TEXT, "
            "telefone_2 TEXT, ddd_fax TEXT, fax TEXT, correio_eletronico TEXT, "
            "situacao_especial TEXT, data_situacao_especial TEXT, snapshot_date TEXT"
        )

        try:
            with self._cursor_scope(conn) as cur:
                self._copy_df_to_temp(cur, staged, temp, ddl)
                cur.execute(f"""
                    INSERT INTO {self.schema}.estabelecimentos (
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
                    SELECT
                        NULLIF(cnpj_completo, ''),
                        NULLIF(cnpj_basico, ''),
                        NULLIF(cnpj_ordem, ''),
                        NULLIF(cnpj_dv, ''),
                        NULLIF(identificador_matriz_filial, ''),
                        NULLIF(nome_fantasia, ''),
                        NULLIF(situacao_cadastral, ''),
                        NULLIF(data_situacao_cadastral, '')::DATE,
                        NULLIF(motivo_situacao_cadastral, ''),
                        NULLIF(nome_cidade_exterior, ''),
                        NULLIF(pais, ''),
                        NULLIF(data_inicio_atividade, '')::DATE,
                        NULLIF(cnae_fiscal, ''),
                        NULLIF(cnae_fiscal_secundaria, ''),
                        NULLIF(tipo_logradouro, ''),
                        NULLIF(logradouro, ''),
                        NULLIF(numero, ''),
                        NULLIF(complemento, ''),
                        NULLIF(bairro, ''),
                        NULLIF(cep, ''),
                        NULLIF(uf, ''),
                        NULLIF(municipio, ''),
                        NULLIF(ddd_1, ''),
                        NULLIF(telefone_1, ''),
                        NULLIF(ddd_2, ''),
                        NULLIF(telefone_2, ''),
                        NULLIF(ddd_fax, ''),
                        NULLIF(fax, ''),
                        NULLIF(correio_eletronico, ''),
                        NULLIF(situacao_especial, ''),
                        NULLIF(data_situacao_especial, '')::DATE,
                        snapshot_date::DATE,
                        NOW()
                    FROM {temp}
                    WHERE NULLIF(cnpj_completo, '') IS NOT NULL
                    ON CONFLICT (cnpj_completo) DO UPDATE SET
                        cnpj_basico                 = EXCLUDED.cnpj_basico,
                        cnpj_ordem                  = EXCLUDED.cnpj_ordem,
                        cnpj_dv                     = EXCLUDED.cnpj_dv,
                        identificador_matriz_filial = EXCLUDED.identificador_matriz_filial,
                        nome_fantasia               = EXCLUDED.nome_fantasia,
                        situacao_cadastral          = EXCLUDED.situacao_cadastral,
                        data_situacao_cadastral     = EXCLUDED.data_situacao_cadastral,
                        motivo_situacao_cadastral   = EXCLUDED.motivo_situacao_cadastral,
                        nome_cidade_exterior        = EXCLUDED.nome_cidade_exterior,
                        pais                        = EXCLUDED.pais,
                        data_inicio_atividade       = EXCLUDED.data_inicio_atividade,
                        cnae_fiscal                 = EXCLUDED.cnae_fiscal,
                        cnae_fiscal_secundaria      = EXCLUDED.cnae_fiscal_secundaria,
                        tipo_logradouro             = EXCLUDED.tipo_logradouro,
                        logradouro                  = EXCLUDED.logradouro,
                        numero                      = EXCLUDED.numero,
                        complemento                 = EXCLUDED.complemento,
                        bairro                      = EXCLUDED.bairro,
                        cep                         = EXCLUDED.cep,
                        uf                          = EXCLUDED.uf,
                        municipio                   = EXCLUDED.municipio,
                        ddd_1                       = EXCLUDED.ddd_1,
                        telefone_1                  = EXCLUDED.telefone_1,
                        ddd_2                       = EXCLUDED.ddd_2,
                        telefone_2                  = EXCLUDED.telefone_2,
                        ddd_fax                     = EXCLUDED.ddd_fax,
                        fax                         = EXCLUDED.fax,
                        correio_eletronico          = EXCLUDED.correio_eletronico,
                        situacao_especial           = EXCLUDED.situacao_especial,
                        data_situacao_especial      = EXCLUDED.data_situacao_especial,
                        snapshot_date               = EXCLUDED.snapshot_date,
                        data_carga                  = NOW()
                    RETURNING (xmax = 0)
                """)
                results = cur.fetchall()
                inserted = sum(1 for r in results if r[0])
                updated = sum(1 for r in results if not r[0])
                return inserted, updated
        except Exception as e:
            logger.error("Erro no bulk_upsert_estabelecimentos: {}", e)
            raise

    # ------------------------------------------------------------------
    # Bulk upsert — simples
    # ------------------------------------------------------------------

    def bulk_upsert_simples(
        self,
        df: pd.DataFrame,
        snapshot_date: date,
        conn: Optional[psycopg2.extensions.connection] = None,
    ) -> Tuple[int, int]:
        if df.empty:
            return 0, 0

        cols = [
            "cnpj_basico", "opcao_pelo_simples",
            "data_opcao_simples", "data_exclusao_simples",
            "opcao_mei", "data_opcao_mei", "data_exclusao_mei",
        ]
        staged = df[cols].copy()
        staged["cnpj_basico"] = staged["cnpj_basico"].astype(str).str.strip().str.zfill(8)
        staged = staged[
            staged["cnpj_basico"].notna()
            & (staged["cnpj_basico"] != "")
            & (staged["cnpj_basico"] != "nan")
        ]
        staged = staged.astype(object).where(pd.notnull(staged), None)

        for col in ("data_opcao_simples", "data_exclusao_simples", "data_opcao_mei", "data_exclusao_mei"):
            staged[col] = staged[col].apply(self._valid_iso_date_str)
        for col in cols:
            staged[col] = staged[col].apply(
                lambda v: None if (not pd.notna(v) or (isinstance(v, str) and v in _NULL_STRS))
                else str(v)
            )
        staged = staged.replace(list(_NULL_STRS), None)

        staged["snapshot_date"] = str(snapshot_date)
        staged = staged[cols + ["snapshot_date"]]

        temp = f"tmp_simples_{uuid.uuid4().hex[:10]}"
        ddl = (
            "cnpj_basico TEXT, opcao_pelo_simples TEXT, data_opcao_simples TEXT, "
            "data_exclusao_simples TEXT, opcao_mei TEXT, data_opcao_mei TEXT, "
            "data_exclusao_mei TEXT, snapshot_date TEXT"
        )

        try:
            with self._cursor_scope(conn) as cur:
                self._copy_df_to_temp(cur, staged, temp, ddl)
                cur.execute(f"""
                    INSERT INTO {self.schema}.simples (
                        cnpj_basico, opcao_pelo_simples,
                        data_opcao_simples, data_exclusao_simples,
                        opcao_mei, data_opcao_mei, data_exclusao_mei,
                        snapshot_date, data_carga
                    )
                    SELECT
                        NULLIF(cnpj_basico, ''),
                        NULLIF(opcao_pelo_simples, ''),
                        NULLIF(data_opcao_simples, '')::DATE,
                        NULLIF(data_exclusao_simples, '')::DATE,
                        NULLIF(opcao_mei, ''),
                        NULLIF(data_opcao_mei, '')::DATE,
                        NULLIF(data_exclusao_mei, '')::DATE,
                        snapshot_date::DATE,
                        NOW()
                    FROM {temp}
                    WHERE NULLIF(cnpj_basico, '') IS NOT NULL
                    ON CONFLICT (cnpj_basico) DO UPDATE SET
                        opcao_pelo_simples    = EXCLUDED.opcao_pelo_simples,
                        data_opcao_simples    = EXCLUDED.data_opcao_simples,
                        data_exclusao_simples = EXCLUDED.data_exclusao_simples,
                        opcao_mei             = EXCLUDED.opcao_mei,
                        data_opcao_mei        = EXCLUDED.data_opcao_mei,
                        data_exclusao_mei     = EXCLUDED.data_exclusao_mei,
                        snapshot_date         = EXCLUDED.snapshot_date,
                        data_carga            = NOW()
                    RETURNING (xmax = 0)
                """)
                results = cur.fetchall()
                inserted = sum(1 for r in results if r[0])
                updated = sum(1 for r in results if not r[0])
                return inserted, updated
        except Exception as e:
            logger.error("Erro no bulk_upsert_simples: {}", e)
            raise

    # ------------------------------------------------------------------
    # Bulk insert — socios (DELETE + INSERT; sem chave natural única)
    # ------------------------------------------------------------------

    def bulk_insert_socios(
        self,
        df: pd.DataFrame,
        snapshot_date: date,
        conn: Optional[psycopg2.extensions.connection] = None,
    ) -> int:
        if df.empty:
            return 0

        cols = [
            "cnpj_basico", "identificador_socio", "nome_socio_razao_social",
            "cnpj_cpf_socio", "qualificacao_socio", "data_entrada_sociedade",
            "pais", "representante_legal", "nome_representante",
            "qualificacao_representante_legal", "faixa_etaria",
        ]
        staged = df[cols].copy()
        staged["cnpj_basico"] = staged["cnpj_basico"].astype(str).str.strip().str.zfill(8)
        staged = staged[
            staged["cnpj_basico"].notna()
            & (staged["cnpj_basico"] != "")
            & (staged["cnpj_basico"] != "nan")
        ]
        staged = staged.astype(object).where(pd.notnull(staged), None)

        for col in cols:
            if col == "data_entrada_sociedade":
                staged[col] = staged[col].apply(self._valid_iso_date_str)
            else:
                staged[col] = staged[col].apply(
                    lambda v: None if (not pd.notna(v) or (isinstance(v, str) and v in _NULL_STRS))
                    else str(v)
                )
        staged = staged.replace(list(_NULL_STRS), None)

        limits = {
            "cnpj_basico": 8, "identificador_socio": 1,
            "nome_socio_razao_social": 150, "cnpj_cpf_socio": 14,
            "qualificacao_socio": 2, "pais": 3,
            "representante_legal": 11, "nome_representante": 60,
            "qualificacao_representante_legal": 2, "faixa_etaria": 1,
        }
        staged = self._truncate_cols(staged, limits)
        staged["nome_socio_razao_social"] = staged["nome_socio_razao_social"].apply(
            lambda v: v if isinstance(v, str) and v else ""
        )
        staged["snapshot_date"] = str(snapshot_date)
        staged = staged[cols + ["snapshot_date"]]

        cnpj_list = staged["cnpj_basico"].dropna().unique().tolist()
        if not cnpj_list:
            return 0

        temp = f"tmp_socios_{uuid.uuid4().hex[:10]}"
        ddl = (
            "cnpj_basico TEXT, identificador_socio TEXT, nome_socio_razao_social TEXT, "
            "cnpj_cpf_socio TEXT, qualificacao_socio TEXT, data_entrada_sociedade TEXT, "
            "pais TEXT, representante_legal TEXT, nome_representante TEXT, "
            "qualificacao_representante_legal TEXT, faixa_etaria TEXT, snapshot_date TEXT"
        )

        target = f"{self.schema}.socios"

        def _execute(cur: psycopg2.extensions.cursor) -> int:
            self._copy_df_to_temp(cur, staged, temp, ddl)

            # DELETE em lotes de 1000 para não exceder tamanho de query
            _DELETE_BATCH = 1000
            for i in range(0, len(cnpj_list), _DELETE_BATCH):
                batch = cnpj_list[i: i + _DELETE_BATCH]
                cur.execute(
                    f"DELETE FROM {target} "
                    f"WHERE snapshot_date = %s AND cnpj_basico = ANY(%s)",
                    (str(snapshot_date), batch),
                )

            cur.execute(f"""
                INSERT INTO {target} (
                    cnpj_basico, identificador_socio, nome_socio_razao_social,
                    cnpj_cpf_socio, qualificacao_socio, data_entrada_sociedade,
                    pais, representante_legal, nome_representante,
                    qualificacao_representante_legal, faixa_etaria,
                    snapshot_date, data_carga
                )
                SELECT
                    NULLIF(cnpj_basico, ''),
                    NULLIF(identificador_socio, ''),
                    COALESCE(NULLIF(nome_socio_razao_social, ''), ''),
                    NULLIF(cnpj_cpf_socio, ''),
                    NULLIF(qualificacao_socio, ''),
                    NULLIF(data_entrada_sociedade, '')::DATE,
                    NULLIF(pais, ''),
                    NULLIF(representante_legal, ''),
                    NULLIF(nome_representante, ''),
                    NULLIF(qualificacao_representante_legal, ''),
                    NULLIF(faixa_etaria, ''),
                    snapshot_date::DATE,
                    NOW()
                FROM {temp}
                WHERE NULLIF(cnpj_basico, '') IS NOT NULL
            """)
            return len(staged)

        if conn is not None:
            try:
                with self._cursor_scope(conn) as cur:
                    return _execute(cur)
            except Exception as e:
                logger.error("Erro no bulk_insert_socios: {}", e)
                raise

        for attempt in Retrying(
            retry=retry_if_exception_type(psycopg2.OperationalError),
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=30, max=300),
            reraise=True,
        ):
            with attempt:
                try:
                    with self._cursor_scope() as cur:
                        return _execute(cur)
                except psycopg2.OperationalError as e:
                    logger.warning("Erro de conexão em bulk_insert_socios (tentando novamente): {}", e)
                    raise
                except Exception as e:
                    logger.error("Erro no bulk_insert_socios: {}", e)
                    raise

    def analyze_groups(self, groups: List[str]) -> None:
        group_to_table = {
            "Cnaes": "cnaes",
            "Motivos": "motivos",
            "Municipios": "municipios",
            "Naturezas": "naturezas",
            "Paises": "paises",
            "Qualificacoes": "qualificacoes",
            "Empresas": "empresas",
            "Estabelecimentos": "estabelecimentos",
            "Socios": "socios",
            "Simples": "simples",
        }
        tables = sorted({group_to_table[g] for g in groups if g in group_to_table})
        if not tables:
            return

        with self.connect(autocommit=True) as conn:
            with conn.cursor() as cur:
                for table in tables:
                    qualified = f"{self.schema}.{table}"
                    logger.info("Atualizando estatísticas: ANALYZE {}", qualified)
                    cur.execute(f"ANALYZE {qualified}")

    # ------------------------------------------------------------------
    # Informações do banco
    # ------------------------------------------------------------------

    def get_database_info(self) -> Dict[str, Any]:
        try:
            rows = self.execute_query("SELECT current_database(), version()")
            tables = self.execute_query(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = %s",
                (self.schema,),
            )
            r = rows[0]
            return {
                "database_name": r[0],
                "sql_version": r[1],
                "server_name": self.host,
                "size_mb": None,
                "table_count": tables[0][0] if tables else 0,
            }
        except Exception as e:
            logger.error("Erro ao obter informações do banco: {}", e)
            return {}
