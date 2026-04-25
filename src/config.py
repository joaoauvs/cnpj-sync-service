"""
Central configuration for the RF CNPJ scraping pipeline.

All tuneable constants live here so operators can adjust behaviour
without touching business logic.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT_DIR: Final[Path] = Path(__file__).resolve().parent.parent
DATA_DIR: Final[Path] = ROOT_DIR / "data"
DOWNLOADS_DIR: Final[Path] = DATA_DIR / "downloads"
EXTRACTED_DIR: Final[Path] = DATA_DIR / "extracted"
PROCESSED_DIR: Final[Path] = DATA_DIR / "processed"
LOGS_DIR: Final[Path] = ROOT_DIR / "logs"

for _d in (DOWNLOADS_DIR, EXTRACTED_DIR, PROCESSED_DIR, LOGS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Remote sources (primary and fallback)
# ---------------------------------------------------------------------------

# Receita Federal — WebDAV via Nextcloud (share público)
# RF_SHARE_TOKEN é o token do share público da RF; pode ser sobrescrito via env var RF_SHARE_TOKEN
import os as _os
RF_SHARE_TOKEN: Final[str] = _os.getenv("RF_SHARE_TOKEN", "YggdBLfdninEJX9")
RF_WEBDAV_BASE: Final[str] = "https://arquivos.receitafederal.gov.br/public.php/webdav"
RF_AUTH: Final[tuple[str, str]] = (RF_SHARE_TOKEN, "")  # Basic Auth: (token, "")

# Fallback source: mirror HTML (usado se WebDAV falhar)
FALLBACK_BASE_URL: Final[str] = "https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/"

# Mantido para compatibilidade interna
PRIMARY_BASE_URL: Final[str] = RF_WEBDAV_BASE

# ---------------------------------------------------------------------------
# HTTP settings
# ---------------------------------------------------------------------------

REQUEST_TIMEOUT: Final[int] = 120         # seconds per request
DOWNLOAD_CHUNK_SIZE: Final[int] = 4 * 1024 * 1024  # 4 MB streaming chunks (melhor throughput)
MAX_RETRIES: Final[int] = 5
BACKOFF_FACTOR: Final[float] = 2.0        # exponential back-off multiplier

HEADERS: Final[dict[str, str]] = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; RFCNPJScraper/1.0; "
        "+https://github.com/joaoauvs/cnpj-sync-service)"
    ),
    "Accept-Encoding": "gzip, deflate",
}

# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------

# Thread pool configuration for different operation types
DOWNLOAD_WORKERS: Final[int] = 12          # parallel download threads (network-bound)
EXTRACT_WORKERS: Final[int] = 4           # parallel extraction threads (CPU/disk-bound)
PROCESS_WORKERS: Final[int] = 4           # parallel CSV processing threads (CPU-bound)
DATABASE_WORKERS: Final[int] = 2          # parallel database load threads (I/O-bound)

# Total workers for different pipeline stages
TOTAL_DOWNLOAD_WORKERS: Final[int] = DOWNLOAD_WORKERS
TOTAL_PROCESS_WORKERS: Final[int] = max(EXTRACT_WORKERS, PROCESS_WORKERS)  # whichever is bottleneck

# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------

CSV_ENCODING: Final[str] = "latin-1"   # RF files use ISO-8859-1
CSV_SEPARATOR: Final[str] = ";"
CSV_CHUNK_ROWS: Final[int] = 200_000   # rows per pandas chunk (memory control)
DB_CHUNK_ROWS: Final[int] = 50_000     # rows per COPY FROM STDIN batch (database load)

# Storage backend configuration
STORAGE_BACKEND: Final[str] = "parquet"  # Options: "csv" or "parquet"

# ---------------------------------------------------------------------------
# File group definitions
# File groups that are partitioned (the server determines how many parts exist)
# ---------------------------------------------------------------------------

PARTITIONED_GROUPS: Final[tuple[str, ...]] = (
    "Empresas",
    "Estabelecimentos",
    "Socios",
)

REFERENCE_FILES: Final[tuple[str, ...]] = (
    "Cnaes",
    "Motivos",
    "Municipios",
    "Naturezas",
    "Paises",
    "Qualificacoes",
)

# ---------------------------------------------------------------------------
# Column schemas (RF CNPJ public data — no headers in source files)
# ---------------------------------------------------------------------------

SCHEMAS: Final[dict[str, list[str]]] = {
    "Empresas": [
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte_empresa",
        "ente_federativo_responsavel",
    ],
    "Estabelecimentos": [
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "pais",
        "data_inicio_atividade",
        "cnae_fiscal",
        "cnae_fiscal_secundaria",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "correio_eletronico",
        "situacao_especial",
        "data_situacao_especial",
    ],
    "Socios": [
        "cnpj_basico",
        "identificador_socio",
        "nome_socio_razao_social",
        "cnpj_cpf_socio",
        "qualificacao_socio",
        "data_entrada_sociedade",
        "pais",
        "representante_legal",
        "nome_representante",
        "qualificacao_representante_legal",
        "faixa_etaria",
    ],
    "Simples": [
        "cnpj_basico",
        "opcao_pelo_simples",
        "data_opcao_simples",
        "data_exclusao_simples",
        "opcao_mei",
        "data_opcao_mei",
        "data_exclusao_mei",
    ],
    "Cnaes": ["codigo", "descricao"],
    "Motivos": ["codigo", "descricao"],
    "Municipios": ["codigo", "descricao"],
    "Naturezas": ["codigo", "descricao"],
    "Paises": ["codigo", "descricao"],
    "Qualificacoes": ["codigo", "descricao"],
}

# ---------------------------------------------------------------------------
# Date columns that need normalisation (YYYYMMDD → YYYY-MM-DD)
# ---------------------------------------------------------------------------

DATE_COLUMNS: Final[dict[str, list[str]]] = {
    "Estabelecimentos": [
        "data_situacao_cadastral",
        "data_inicio_atividade",
        "data_situacao_especial",
    ],
    "Socios": ["data_entrada_sociedade"],
    "Simples": [
        "data_opcao_simples",
        "data_exclusao_simples",
        "data_opcao_mei",
        "data_exclusao_mei",
    ],
}

# ---------------------------------------------------------------------------
# Numeric columns that use Brazilian decimal format (comma → dot)
# ---------------------------------------------------------------------------

DECIMAL_COLUMNS: Final[dict[str, list[str]]] = {
    "Empresas": ["capital_social"],
}
