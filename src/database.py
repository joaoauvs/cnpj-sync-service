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
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

import pandas as pd
import pyodbc

from src.logger import logger


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

class CNPJDatabase(SQLServerConnection):
    """Operações específicas do schema CNPJ."""

    def __init__(self, database: str = "receita-federal", **kwargs):
        super().__init__(database=database, **kwargs)
        self.schema = "cnpj"

    # ------------------------------------------------------------------
    # Inicialização
    # ------------------------------------------------------------------

    def create_database_if_not_exists(self) -> bool:
        try:
            with self.connect_master() as conn:
                cur = conn.cursor()
                cur.execute("SELECT name FROM sys.databases WHERE name = ?", (self.database,))
                if cur.fetchone():
                    logger.info("Banco '{}' já existe", self.database)
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

            logger.info("Script {} executado com sucesso", script_path.name)
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
                    logger.info("Sessão iniciada: id_execucao={}, snapshot={}", exec_id, snapshot_date)
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

        # Garantir colunas limpas
        df = df[["codigo", "descricao"]].copy()
        df["codigo"] = df["codigo"].astype(str).str.strip()
        df["descricao"] = df["descricao"].fillna("").astype(str).str.strip()
        df = df[df["codigo"].notna() & (df["codigo"] != "") & (df["codigo"] != "nan")]

        data = list(df.itertuples(index=False, name=None))
        if not data:
            return 0

        target = f"{self.schema}.{table}"
        try:
            with self.connect() as conn:
                cur = conn.cursor()
                # fast_executemany não é suportado pelo driver "SQL Server" nativo;
                # usar apenas com "ODBC Driver 17/18 for SQL Server"
                if "ODBC Driver" in self.driver:
                    cur.fast_executemany = True

                cur.execute(
                    "CREATE TABLE #tmp_ref (codigo VARCHAR(20), descricao VARCHAR(255))"
                )
                cur.executemany("INSERT INTO #tmp_ref VALUES (?, ?)", data)

                snap_str = str(snapshot_date)
                cur.execute(f"""
                    MERGE {target} AS t
                    USING #tmp_ref AS s ON t.codigo = s.codigo
                    WHEN MATCHED AND t.descricao <> s.descricao THEN
                        UPDATE SET t.descricao = s.descricao,
                                   t.snapshot_date = ?,
                                   t.data_carga = GETDATE()
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT (codigo, descricao, snapshot_date, data_carga)
                        VALUES (s.codigo, s.descricao, ?, GETDATE());
                """, (snap_str, snap_str))

                affected = cur.rowcount
                cur.execute("DROP TABLE IF EXISTS #tmp_ref")
                logger.info("{}: {} registros mesclados", table, len(data))
                return affected
        except Exception as e:
            logger.error("Erro no bulk_upsert_reference [{}]: {}", table, e)
            raise

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

        cols = [
            "cnpj_basico", "razao_social", "natureza_juridica",
            "qualificacao_responsavel", "capital_social",
            "porte_empresa", "ente_federativo_responsavel",
        ]
        df = df[cols].copy()
        df["cnpj_basico"] = df["cnpj_basico"].astype(str).str.strip().str.zfill(8)
        df = df[df["cnpj_basico"].notna() & (df["cnpj_basico"] != "") & (df["cnpj_basico"] != "nan")]
        # .astype(object) garante que NaN em colunas float (ex: capital_social)
        # seja substituído por Python None — fast_executemany não suporta float('nan')
        df = df.astype(object).where(pd.notnull(df), None)

        snap_str = str(snapshot_date)
        data = [(*row, snap_str) for row in df.itertuples(index=False, name=None)]
        if not data:
            return 0, 0

        target = f"{self.schema}.empresas"
        try:
            with self.connect() as conn:
                cur = conn.cursor()
                # fast_executemany não é suportado pelo driver "SQL Server" nativo;
                # usar apenas com "ODBC Driver 17/18 for SQL Server"
                if "ODBC Driver" in self.driver:
                    cur.fast_executemany = True

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
                cur.executemany("INSERT INTO #tmp_emp VALUES (?,?,?,?,?,?,?,?)", data)

                cur.execute(f"""
                    MERGE {target} AS t
                    USING #tmp_emp AS s ON t.cnpj_basico = s.cnpj_basico
                    WHEN MATCHED AND (
                        ISNULL(t.razao_social,'')                <> ISNULL(s.razao_social,'') OR
                        ISNULL(t.natureza_juridica,'')           <> ISNULL(s.natureza_juridica,'') OR
                        ISNULL(t.qualificacao_responsavel,'')    <> ISNULL(s.qualificacao_responsavel,'') OR
                        ISNULL(CAST(t.capital_social AS VARCHAR),'') <> ISNULL(CAST(s.capital_social AS VARCHAR),'') OR
                        ISNULL(t.porte_empresa,'')               <> ISNULL(s.porte_empresa,'') OR
                        ISNULL(t.ente_federativo_responsavel,'') <> ISNULL(s.ente_federativo_responsavel,'')
                    ) THEN
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
        df = df.where(pd.notnull(df), None)

        # Ordem final das colunas para INSERT (deve bater com o #tmp_estab)
        col_order = ["cnpj_completo"] + required + ["snapshot_date"]
        df["snapshot_date"] = str(snapshot_date)
        df = df[col_order]

        data = list(df.itertuples(index=False, name=None))
        if not data:
            return 0, 0

        target = f"{self.schema}.estabelecimentos"
        try:
            with self.connect() as conn:
                cur = conn.cursor()
                # fast_executemany não é suportado pelo driver "SQL Server" nativo;
                # usar apenas com "ODBC Driver 17/18 for SQL Server"
                if "ODBC Driver" in self.driver:
                    cur.fast_executemany = True

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
                cur.executemany(f"INSERT INTO #tmp_estab VALUES ({placeholders})", data)

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

        cols = [
            "cnpj_basico", "opcao_pelo_simples",
            "data_opcao_simples", "data_exclusao_simples",
            "opcao_mei", "data_opcao_mei", "data_exclusao_mei",
        ]
        df = df[cols].copy()
        df["cnpj_basico"] = df["cnpj_basico"].astype(str).str.strip().str.zfill(8)
        df = df[df["cnpj_basico"].notna() & (df["cnpj_basico"] != "") & (df["cnpj_basico"] != "nan")]
        df = df.where(pd.notnull(df), None)
        df["snapshot_date"] = str(snapshot_date)

        data = list(df[cols + ["snapshot_date"]].itertuples(index=False, name=None))
        if not data:
            return 0, 0

        target = f"{self.schema}.simples"
        try:
            with self.connect() as conn:
                cur = conn.cursor()
                # fast_executemany não é suportado pelo driver "SQL Server" nativo;
                # usar apenas com "ODBC Driver 17/18 for SQL Server"
                if "ODBC Driver" in self.driver:
                    cur.fast_executemany = True

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
                cur.executemany("INSERT INTO #tmp_simples VALUES (?,?,?,?,?,?,?,?)", data)

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

        cols = [
            "cnpj_basico", "identificador_socio", "nome_socio_razao_social",
            "cnpj_cpf_socio", "qualificacao_socio", "data_entrada_sociedade",
            "pais", "representante_legal", "nome_representante",
            "qualificacao_representante_legal", "faixa_etaria",
        ]
        df = df[cols].copy()
        df["cnpj_basico"] = df["cnpj_basico"].astype(str).str.strip().str.zfill(8)
        df = df[df["cnpj_basico"].notna() & (df["cnpj_basico"] != "") & (df["cnpj_basico"] != "nan")]
        df = df.where(pd.notnull(df), None)
        # Converter data_entrada_sociedade de string para datetime.date (ou None)
        # para que fast_executemany infira corretamente o tipo DATE no SQL Server.
        df["data_entrada_sociedade"] = df["data_entrada_sociedade"].apply(
            lambda v: date.fromisoformat(v) if isinstance(v, str) and len(v) == 10 else None
        )
        # snapshot_date como objeto date (não string) para colunas DATE do SQL Server
        df["snapshot_date"] = snapshot_date

        if df.empty:
            return 0

        # CNPJ basicos únicos neste lote para o DELETE seletivo
        cnpj_list = df["cnpj_basico"].unique().tolist()
        data = list(df[cols + ["snapshot_date"]].itertuples(index=False, name=None))

        target = f"{self.schema}.socios"
        try:
            with self.connect() as conn:
                cur = conn.cursor()
                # fast_executemany não é suportado pelo driver "SQL Server" nativo;
                # usar apenas com "ODBC Driver 17/18 for SQL Server"
                if "ODBC Driver" in self.driver:
                    cur.fast_executemany = True

                # Deleta apenas os socios deste snapshot para os CNPJs do lote
                # (safe para re-runs sem apagar dados de outros snapshots)
                placeholders = ",".join(["?"] * len(cnpj_list))
                cur.execute(
                    f"DELETE FROM {target} "
                    f"WHERE snapshot_date = ? AND cnpj_basico IN ({placeholders})",
                    (snapshot_date, *cnpj_list),
                )

                placeholders_row = ",".join(["?"] * (len(cols) + 1))
                cur.executemany(
                    f"INSERT INTO {target} "
                    f"({', '.join(cols)}, snapshot_date, data_carga) "
                    f"VALUES ({placeholders_row}, GETDATE())",
                    data,
                )
                return len(data)
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
