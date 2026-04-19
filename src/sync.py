"""
Módulo de sincronização CNPJ com SQL Server.

Orquestra:
  1. Verificação de snapshot (evita reprocessamento)
  2. Download e extração dos arquivos via pipeline
  3. Leitura do CSV + limpeza dos dados
  4. Bulk MERGE/INSERT no SQL Server (via CNPJDatabase)
  5. Atualização do controle de execução
"""

from __future__ import annotations

import shutil
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pandas as pd
import pyarrow.parquet as pq

from src.config import CSV_CHUNK_ROWS, CSV_ENCODING, CSV_SEPARATOR, DATE_COLUMNS, DECIMAL_COLUMNS, HEADERS, RF_AUTH, SCHEMAS, STORAGE_BACKEND
from src.crawler import SnapshotCrawler, discover_latest_snapshot_with_fallback
from src.database import CNPJDatabase
from src.logger_enhanced import logger, structured_logger
from src.models import RemoteFile, Snapshot
from src.pipeline import CNPJPipeline, run_pipeline

class DataFrameNormalizer:
    """Serviço de normalização aplicado antes da carga no banco."""

    @staticmethod
    def _norm_date(val: Any) -> Optional[str]:
        if val is None:
            return None
        try:
            if pd.isna(val):
                return None
        except (TypeError, ValueError):
            pass
        s = str(val).strip()
        if not s:
            return None
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            try:
                date.fromisoformat(s)
                return s
            except ValueError:
                return None
        s = s.zfill(8)
        if len(s) == 8 and s.isdigit() and s != "00000000":
            candidate = f"{s[:4]}-{s[4:6]}-{s[6:8]}"
            try:
                date.fromisoformat(candidate)
                return candidate
            except ValueError:
                return None
        return None

    def apply_date_cols(self, df: pd.DataFrame, group: str) -> pd.DataFrame:
        for col in DATE_COLUMNS.get(group, []):
            if col in df.columns:
                df[col] = df[col].apply(self._norm_date)
        return df

    def apply_decimal_cols(self, df: pd.DataFrame, group: str) -> pd.DataFrame:
        for col in DECIMAL_COLUMNS.get(group, []):
            if col in df.columns:
                df[col] = (
                    df[col].astype(str)
                    .str.replace(",", ".", regex=False)
                    .str.strip()
                    .replace("", None)
                    .pipe(pd.to_numeric, errors="coerce")
                )
        return df

    def clean(self, df: pd.DataFrame, group: str) -> pd.DataFrame:
        df = df.apply(lambda s: s.str.strip() if s.dtype == object else s)
        df = self.apply_date_cols(df, group)
        df = self.apply_decimal_cols(df, group)
        df = df.replace({"": None, "nan": None, "None": None})
        return df


class ProcessedFileReader:
    """Serviço de leitura dos artefatos processados em CSV ou Parquet."""

    def __init__(self, storage_backend: str = STORAGE_BACKEND) -> None:
        self.storage_backend = storage_backend.lower()

    @staticmethod
    def iter_processed_chunks(file_path: Path, chunksize: int) -> Iterator[pd.DataFrame]:
        suffix = file_path.suffix.lower()
        if suffix == ".csv":
            yield from pd.read_csv(
                file_path,
                encoding="utf-8",
                dtype=str,
                chunksize=chunksize,
                on_bad_lines="warn",
            )
            return

        if suffix == ".parquet":
            parquet_file = pq.ParquetFile(file_path)
            for batch in parquet_file.iter_batches(batch_size=chunksize, use_threads=True):
                yield batch.to_pandas()
            return

        raise ValueError(f"Formato de arquivo processado não suportado: {file_path.suffix}")

    def read(self, file_path: Path, chunksize: Optional[int] = None):
        if chunksize:
            return self.iter_processed_chunks(file_path, chunksize)

        if self.storage_backend == "csv":
            return pd.read_csv(
                file_path,
                encoding="utf-8",
                dtype=str,
                chunksize=chunksize,
                on_bad_lines="warn",
            )
        if self.storage_backend == "parquet":
            return pd.read_parquet(file_path, engine="pyarrow")
        raise ValueError(f"Backend de storage desconhecido: {self.storage_backend}")


# ---------------------------------------------------------------------------
# Orquestrador principal
# ---------------------------------------------------------------------------

class CNPJSync:
    """Orquestra a sincronização completa CNPJ → SQL Server."""

    def __init__(
        self,
        db_connection: CNPJDatabase,
        data_dir: Path = Path("data"),
        chunk_size: int = CSV_CHUNK_ROWS,
        pipeline: Optional[CNPJPipeline] = None,
        snapshot_crawler: Optional[SnapshotCrawler] = None,
        file_reader: Optional[ProcessedFileReader] = None,
        normalizer: Optional[DataFrameNormalizer] = None,
    ):
        self.db = db_connection
        self.data_dir = data_dir
        self.chunk_size = chunk_size
        self.pipeline = pipeline or CNPJPipeline()
        self.snapshot_crawler = snapshot_crawler or SnapshotCrawler()
        self.file_reader = file_reader or ProcessedFileReader()
        self.normalizer = normalizer or DataFrameNormalizer()

        self.downloads_dir = data_dir / "downloads"
        self.extracted_dir = data_dir / "extracted"
        self.processed_dir = data_dir / "processed"
        for d in (self.downloads_dir, self.extracted_dir, self.processed_dir):
            d.mkdir(parents=True, exist_ok=True)

        self.current_exec_id: Optional[int] = None
        self.current_snapshot_date: Optional[date] = None
        self.file_tracking: Dict[str, int] = {}

    # ------------------------------------------------------------------
    # Inicialização do banco
    # ------------------------------------------------------------------

    def initialize_database(self) -> bool:
        if not self.db.create_database_if_not_exists():
            logger.error("Falha ao criar banco de dados")
            return False

        script = Path(__file__).parent.parent / "sql" / "schema.sql"
        if not script.exists():
            logger.error("Script não encontrado: {}", script)
            return False
        if not self.db.execute_schema_script(script):
            logger.error("Falha ao executar schema.sql")
            return False
        logger.debug("Schema inicializado via schema.sql")
        return True

    # ------------------------------------------------------------------
    # Controle de sessão
    # ------------------------------------------------------------------

    def check_snapshot_needs_sync(self, snapshot_date: date) -> Tuple[bool, str]:
        """
        Retorna (precisa_sincronizar, motivo).
        False = já processado com sucesso ou em andamento → pular.
        """
        if self.db.check_snapshot_exists(snapshot_date):
            return False, f"Snapshot {snapshot_date} já processado com SUCESSO"
        if self.db.is_snapshot_running(snapshot_date):
            return False, f"Snapshot {snapshot_date} já está EM_EXECUCAO (outra instância?)"
        return True, f"Snapshot {snapshot_date} pendente de sincronização"

    def start_sync_session(self, snapshot_date: date, force: bool = False) -> bool:
        if not force:
            needs, msg = self.check_snapshot_needs_sync(snapshot_date)
            if not needs:
                logger.info(msg)
                return False

        exec_id = self.db.start_sync_session(snapshot_date, force=force)
        if not exec_id:
            logger.error("Falha ao iniciar sessão de sincronização")
            return False

        self.current_exec_id = exec_id
        self.current_snapshot_date = snapshot_date
        self.file_tracking.clear()
        return True

    def _register_file(self, remote_file: RemoteFile) -> Optional[int]:
        if not self.current_exec_id:
            return None
        file_id = self.db.add_file_to_sync(
            exec_id=self.current_exec_id,
            group=remote_file.group,
            filename=remote_file.name,
        )
        if file_id:
            self.file_tracking[remote_file.name] = file_id
        return file_id

    def _update_file(
        self,
        filename: str,
        status: str,
        total: Optional[int] = None,
        invalid: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        file_id = self.file_tracking.get(filename)
        if file_id:
            self.db.update_file_status(file_id, status, total, invalid, error)

    # ------------------------------------------------------------------
    # Processadores por grupo
    # ------------------------------------------------------------------

    def _read_processed_file(self, file_path: Path, group: str, chunksize: Optional[int] = None):
        """
        Lê um arquivo processado pelo pipeline (CSV ou Parquet).

        O pipeline escreve arquivos com:
          - CSV: separador vírgula, encoding UTF-8, cabeçalho na primeira linha
          - Parquet: formato Apache Parquet com pyarrow
        """
        return self.file_reader.read(file_path, chunksize=chunksize)

    def _load_reference(self, group: str, file_path: Path, snapshot_date: date) -> int:
        """Carrega tabela de referência (dimensão pequena) no banco."""
        logger.info("Carregando referência {}: {}", group, file_path.name)
        try:
            df = self._read_processed_file(file_path, group)
            df = self.normalizer.clean(df, group)
            affected = self.db.bulk_upsert_reference(
                table=group.lower(),
                df=df,
                snapshot_date=snapshot_date,
            )
            logger.info("{}: {} linhas mescladas", group, affected)
            return affected
        except Exception as e:
            logger.error("Erro ao carregar {}: {}", group, e)
            raise

    def _load_empresas(self, file_path: Path, snapshot_date: date) -> Tuple[int, int]:
        """Carrega empresas em chunks via bulk MERGE."""
        logger.info("Carregando empresas: {}", file_path.name)
        total_inserted = total_updated = 0
        try:
            reader = self._read_processed_file(file_path, "Empresas", chunksize=self.chunk_size)
            for chunk in reader:
                chunk = self.normalizer.clean(chunk, "Empresas")
                ins, upd = self.db.bulk_upsert_empresas(chunk, snapshot_date)
                total_inserted += ins
                total_updated += upd
            logger.info("Empresas: {} inseridas, {} atualizadas", total_inserted, total_updated)
            return total_inserted, total_updated
        except Exception as e:
            logger.error("Erro ao carregar empresas: {}", e)
            raise

    def _load_estabelecimentos(self, file_path: Path, snapshot_date: date) -> Tuple[int, int]:
        """Carrega estabelecimentos em chunks via bulk MERGE."""
        logger.info("Carregando estabelecimentos: {}", file_path.name)
        total_inserted = total_updated = 0
        try:
            reader = self._read_processed_file(file_path, "Estabelecimentos", chunksize=self.chunk_size)
            for chunk in reader:
                chunk = self.normalizer.clean(chunk, "Estabelecimentos")
                ins, upd = self.db.bulk_upsert_estabelecimentos(chunk, snapshot_date)
                total_inserted += ins
                total_updated += upd
            logger.info("Estabelecimentos: {} inseridos, {} atualizados", total_inserted, total_updated)
            return total_inserted, total_updated
        except Exception as e:
            logger.error("Erro ao carregar estabelecimentos: {}", e)
            raise

    def _load_socios(self, file_path: Path, snapshot_date: date) -> int:
        """Carrega sócios em chunks (DELETE + INSERT por lote de cnpj_basico)."""
        logger.info("Carregando socios: {}", file_path.name)
        total = 0
        try:
            reader = self._read_processed_file(file_path, "Socios", chunksize=self.chunk_size)
            for chunk in reader:
                chunk = self.normalizer.clean(chunk, "Socios")
                n = self.db.bulk_insert_socios(chunk, snapshot_date)
                total += n
            logger.info("Socios: {} inseridos", total)
            return total
        except Exception as e:
            logger.error("Erro ao carregar socios: {}", e)
            raise

    def _load_simples(self, file_path: Path, snapshot_date: date) -> Tuple[int, int]:
        """Carrega Simples Nacional em chunks via bulk MERGE."""
        logger.info("Carregando simples: {}", file_path.name)
        total_inserted = total_updated = 0
        try:
            reader = self._read_processed_file(file_path, "Simples", chunksize=self.chunk_size)
            for chunk in reader:
                chunk = self.normalizer.clean(chunk, "Simples")
                ins, upd = self.db.bulk_upsert_simples(chunk, snapshot_date)
                total_inserted += ins
                total_updated += upd
            logger.info("Simples: {} inseridos, {} atualizados", total_inserted, total_updated)
            return total_inserted, total_updated
        except Exception as e:
            logger.error("Erro ao carregar simples: {}", e)
            raise

    # ------------------------------------------------------------------
    # Dispatcher por grupo
    # ------------------------------------------------------------------

    REFERENCE_GROUPS = {"Cnaes", "Motivos", "Municipios", "Naturezas", "Paises", "Qualificacoes"}

    def _dispatch_group(self, group: str, file_path: Path, snapshot_date: date) -> int:
        """
        Chama o loader correto para cada grupo.
        Retorna total de registros processados.
        """
        if group in self.REFERENCE_GROUPS:
            return self._load_reference(group, file_path, snapshot_date)
        if group == "Empresas":
            ins, upd = self._load_empresas(file_path, snapshot_date)
            return ins + upd
        if group == "Estabelecimentos":
            ins, upd = self._load_estabelecimentos(file_path, snapshot_date)
            return ins + upd
        if group == "Socios":
            return self._load_socios(file_path, snapshot_date)
        if group == "Simples":
            ins, upd = self._load_simples(file_path, snapshot_date)
            return ins + upd
        logger.warning("Grupo desconhecido ignorado: {}", group)
        return 0

    # ------------------------------------------------------------------
    # Pipeline principal
    # ------------------------------------------------------------------

    def sync_snapshot(
        self,
        snapshot_date: Optional[date] = None,
        groups: Optional[List[str]] = None,
        force_download: bool = False,
        force_extract: bool = False,
        download_workers: int = 4,
        process_workers: int = 4,
        reference_only: bool = False,
        force: bool = False,
        reuse_processed: bool = False,
    ) -> Dict[str, Any]:
        """
        Sincroniza um snapshot completo.

        Fluxo:
          1. Descobre o snapshot (uma única chamada ao crawler)
          2. Verifica idempotência
          3. Download → extração → processamento
          4. Carga no SQL Server via bulk MERGE
          5. Atualiza controle_sincronizacao
          6. Remove arquivos temporários se sucesso
        """
        import requests as _req
        _session = self.snapshot_crawler.create_session()
        _session.auth = RF_AUTH
        try:
            snapshot_obj: Snapshot = self.snapshot_crawler.discover_latest_snapshot_with_fallback(session=_session)
        finally:
            _session.close()

        # Resolve data alvo: usa a do env/argumento ou a do snapshot descoberto
        if snapshot_date is None:
            raw = snapshot_obj.date
            snapshot_date = datetime.strptime(raw + "-01", "%Y-%m-%d").date() if len(raw) == 7 \
                else datetime.strptime(raw, "%Y-%m-%d").date()

        logger.info("=== INÍCIO SINCRONIZAÇÃO snapshot={} ===", snapshot_obj.date)

        # Verificação de idempotência
        if not force:
            needs, msg = self.check_snapshot_needs_sync(snapshot_date)
            if not needs:
                logger.info("{}", msg)
                return {"success": True, "skipped": True, "message": msg, "snapshot_date": str(snapshot_date)}

        if not self.start_sync_session(snapshot_date, force=force):
            return {
                "success": False,
                "message": "Falha ao iniciar sessão de sincronização",
                "snapshot_date": str(snapshot_date),
            }

        total_files = successful_files = failed_files = 0
        total_records = 0

        try:
            # 1. Executar pipeline com o snapshot já descoberto (sem nova chamada ao crawler)
            pipeline_result = self.pipeline.run(
                groups=groups,
                snapshot_date=str(snapshot_date),
                force_download=force_download,
                force_extract=force_extract,
                download_workers=download_workers,
                process_workers=process_workers,
                reference_only=reference_only,
                snapshot=snapshot_obj,
                reuse_processed=reuse_processed,
            )

            total_files = len(pipeline_result.results)

            # 2. Carregar cada arquivo processado no SQL Server
            for pr in pipeline_result.results:
                remote_file = pr.extraction_result.download_result.remote_file
                filename = remote_file.name
                group = remote_file.group

                self._register_file(remote_file)

                if pr.status.value != "done" or not pr.output_path or not pr.output_path.exists():
                    self._update_file(filename, "FALHA", error=pr.error)
                    logger.error("Arquivo {} falhou no pipeline: {}", filename, pr.error)
                    failed_files += 1
                    continue

                try:
                    records = self._dispatch_group(group, pr.output_path, snapshot_date)
                    self._update_file(filename, "SUCESSO", total=records, invalid=pr.rows_invalid)
                    total_records += records
                    successful_files += 1
                except Exception as e:
                    self._update_file(filename, "FALHA", error=str(e))
                    logger.error("Erro ao carregar {} no banco: {}", filename, e)
                    failed_files += 1

            # 3. Atualizar controle
            final_status = "SUCESSO" if failed_files == 0 else "FALHA"
            self.db.update_sync_session(
                exec_id=self.current_exec_id,
                status=final_status,
                total_files=total_files,
                processed_files=successful_files,
                failed_files=failed_files,
                total_records=total_records,
            )

            result = {
                "success": failed_files == 0,
                "snapshot_date": str(snapshot_date),
                "execution_id": self.current_exec_id,
                "total_files": total_files,
                "successful_files": successful_files,
                "failed_files": failed_files,
                "total_records": total_records,
            }

            logger.info("=== FIM SINCRONIZAÇÃO: {}/{} arquivos ok, {:,} registros ===",
                        successful_files, total_files, total_records)

            if failed_files == 0:
                self._cleanup_temp_files(snapshot_date)

            return result

        except Exception as e:
            logger.error("Erro crítico na sincronização: {}", e)
            if self.current_exec_id:
                self.db.update_sync_session(
                    exec_id=self.current_exec_id,
                    status="FALHA",
                    error_message=str(e),
                )
            return {
                "success": False,
                "message": str(e),
                "snapshot_date": str(snapshot_date),
                "execution_id": self.current_exec_id,
            }
        finally:
            self.current_exec_id = None
            self.current_snapshot_date = None
            self.file_tracking.clear()

    # ------------------------------------------------------------------
    # Limpeza
    # ------------------------------------------------------------------

    def _cleanup_temp_files(self, snapshot_date: date) -> None:
        from src.config import DOWNLOADS_DIR, EXTRACTED_DIR, PROCESSED_DIR

        snap_str = str(snapshot_date)
        for base_dir in (DOWNLOADS_DIR, EXTRACTED_DIR):
            target = base_dir / snap_str
            if target.exists():
                shutil.rmtree(target, ignore_errors=True)
                logger.info("Removido: {}", target)

        # Processed: remove arquivos carregados no DB
        processed_snap = PROCESSED_DIR / snap_str
        if processed_snap.exists():
            shutil.rmtree(processed_snap, ignore_errors=True)
            logger.info("Removido: {}", processed_snap)

    # ------------------------------------------------------------------
    # Status (observabilidade)
    # ------------------------------------------------------------------

    def get_sync_status(self, exec_id: Optional[int] = None) -> Dict[str, Any]:
        try:
            if exec_id is None:
                rows = self.db.execute_query(
                    f"SELECT TOP 1 id_execucao FROM {self.db.schema}.controle_sincronizacao "
                    "ORDER BY data_inicio_execucao DESC"
                )
                if not rows:
                    return {"error": "Nenhuma execução encontrada"}
                exec_id = rows[0][0]

            rows = self.db.execute_query(
                f"SELECT id_execucao, snapshot_date, status, data_inicio_execucao, "
                "data_fim_execucao, total_arquivos, arquivos_processados, arquivos_falha, "
                "total_registros, duracao_segundos, erro_mensagem "
                f"FROM {self.db.schema}.controle_sincronizacao WHERE id_execucao = ?",
                (exec_id,),
            )
            if not rows:
                return {"error": f"Execução {exec_id} não encontrada"}

            r = rows[0]
            result: Dict[str, Any] = {
                "execution_id": r[0],
                "snapshot_date": str(r[1]),
                "status": r[2],
                "start_time": str(r[3]),
                "end_time": str(r[4]) if r[4] else None,
                "total_files": r[5],
                "processed_files": r[6],
                "failed_files": r[7],
                "total_records": r[8],
                "duration_seconds": r[9],
                "error_message": r[10],
            }

            files = self.db.execute_query(
                f"SELECT id_arquivo, grupo_arquivo, nome_arquivo, status, "
                "data_inicio, data_fim, total_registros, registros_invalidos, erro_mensagem "
                f"FROM {self.db.schema}.controle_arquivos WHERE id_execucao = ? "
                "ORDER BY data_inicio",
                (exec_id,),
            )
            result["files"] = [
                {
                    "file_id": f[0], "group": f[1], "filename": f[2],
                    "status": f[3], "start_time": str(f[4]),
                    "end_time": str(f[5]) if f[5] else None,
                    "total_records": f[6], "invalid_records": f[7],
                    "error_message": f[8],
                }
                for f in files
            ]
            return result
        except Exception as e:
            logger.error("Erro ao obter status: {}", e)
            return {"error": str(e)}
