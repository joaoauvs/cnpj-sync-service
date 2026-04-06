# =============================================================================
# CNPJ Sync Service — Imagem de Produção
#
# Inclui:
#   - Microsoft ODBC Driver 18 for SQL Server
#   - Driver "ODBC Driver 18 for SQL Server" (name usado em database.py)
#   - Dependências Python do projeto
#
# Build:
#   docker build -t cnpj-sync-service .
#
# Run (exemplo):
#   docker run --rm \
#     -e DB_SERVER=172.0.0.1 \
#     -e DB_DATABASE=receita-federal \
#     -e DB_USERNAME=sa \
#     -e DB_PASSWORD=SuaSenha \
#     -v /mnt/data:/app/data \
#     cnpj-sync-service
# =============================================================================

FROM python:3.11-slim AS base

# ---------------------------------------------------------------------------
# Dependências de sistema: ODBC Driver 18 para SQL Server
# ---------------------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        gnupg \
        apt-transport-https \
        unixodbc \
        unixodbc-dev \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] \
        https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Dependências Python
# ---------------------------------------------------------------------------
WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ---------------------------------------------------------------------------
# Código da aplicação
# ---------------------------------------------------------------------------
COPY src/       ./src/
COPY sql/       ./sql/
COPY main.py    .

# Diretórios de dados e logs (montar volume externo em produção)
RUN mkdir -p data/downloads data/extracted data/processed logs

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DB_SERVER="" \
    DB_DATABASE="receita-federal" \
    DB_USERNAME="" \
    DB_PASSWORD=""

# Healthcheck simples: verifica que o módulo Python importa corretamente
HEALTHCHECK --interval=60s --timeout=10s --retries=3 \
    CMD python -c "from src.database import CNPJDatabase; print('ok')" || exit 1

ENTRYPOINT ["python", "main.py"]
CMD []
