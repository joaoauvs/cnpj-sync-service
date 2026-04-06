-- =============================================================================
-- Schema de PRODUÇÃO — CNPJ Sync Service
-- SQL Server 2019+
--
-- Diferenças em relação ao schema_final.sql:
--   * Sem FK constraints: evita falhas por integridade referencial nos dados
--     brutos da RF (códigos de CNAE, município, país inválidos são comuns).
--   * Índices otimizados para queries de leitura e para o padrão de carga.
--   * controle_sincronizacao: índice UNIQUE em (snapshot_date, status) para
--     prevenir concorrência duplicada.
--   * Todas as colunas NOT NULL que aceitam vazio usam '' como default nos
--     índices — evita problemas com NULL em índices compostos.
--   * Compatível com execuções idempotentes: todos os CREATE são IF NOT EXISTS.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Schema
-- ---------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'cnpj')
    EXEC('CREATE SCHEMA cnpj');
GO

-- =============================================================================
-- TABELAS DE REFERÊNCIA (dimensões pequenas, ~centenas de linhas)
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.tables t
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'cnpj' AND t.name = 'cnaes')
BEGIN
    CREATE TABLE cnpj.cnaes (
        codigo      VARCHAR(10)  NOT NULL,
        descricao   VARCHAR(255) NOT NULL,
        snapshot_date DATE       NOT NULL,
        data_carga  DATETIME     NOT NULL DEFAULT GETDATE(),
        CONSTRAINT pk_cnaes PRIMARY KEY CLUSTERED (codigo)
    ) WITH (DATA_COMPRESSION = ROW);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables t
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'cnpj' AND t.name = 'motivos')
BEGIN
    CREATE TABLE cnpj.motivos (
        codigo      VARCHAR(2)   NOT NULL,
        descricao   VARCHAR(255) NOT NULL,
        snapshot_date DATE       NOT NULL,
        data_carga  DATETIME     NOT NULL DEFAULT GETDATE(),
        CONSTRAINT pk_motivos PRIMARY KEY CLUSTERED (codigo)
    ) WITH (DATA_COMPRESSION = ROW);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables t
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'cnpj' AND t.name = 'municipios')
BEGIN
    CREATE TABLE cnpj.municipios (
        codigo      VARCHAR(4)   NOT NULL,
        descricao   VARCHAR(255) NOT NULL,
        snapshot_date DATE       NOT NULL,
        data_carga  DATETIME     NOT NULL DEFAULT GETDATE(),
        CONSTRAINT pk_municipios PRIMARY KEY CLUSTERED (codigo)
    ) WITH (DATA_COMPRESSION = ROW);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables t
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'cnpj' AND t.name = 'naturezas')
BEGIN
    CREATE TABLE cnpj.naturezas (
        codigo      VARCHAR(4)   NOT NULL,
        descricao   VARCHAR(255) NOT NULL,
        snapshot_date DATE       NOT NULL,
        data_carga  DATETIME     NOT NULL DEFAULT GETDATE(),
        CONSTRAINT pk_naturezas PRIMARY KEY CLUSTERED (codigo)
    ) WITH (DATA_COMPRESSION = ROW);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables t
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'cnpj' AND t.name = 'paises')
BEGIN
    CREATE TABLE cnpj.paises (
        codigo      VARCHAR(3)   NOT NULL,
        descricao   VARCHAR(255) NOT NULL,
        snapshot_date DATE       NOT NULL,
        data_carga  DATETIME     NOT NULL DEFAULT GETDATE(),
        CONSTRAINT pk_paises PRIMARY KEY CLUSTERED (codigo)
    ) WITH (DATA_COMPRESSION = ROW);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables t
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'cnpj' AND t.name = 'qualificacoes')
BEGIN
    CREATE TABLE cnpj.qualificacoes (
        codigo      VARCHAR(2)   NOT NULL,
        descricao   VARCHAR(255) NOT NULL,
        snapshot_date DATE       NOT NULL,
        data_carga  DATETIME     NOT NULL DEFAULT GETDATE(),
        CONSTRAINT pk_qualificacoes PRIMARY KEY CLUSTERED (codigo)
    ) WITH (DATA_COMPRESSION = ROW);
END
GO

-- =============================================================================
-- TABELAS PRINCIPAIS
-- Estratégia: manter apenas o snapshot mais recente por chave natural.
-- A coluna snapshot_date indica qual carga gerou/atualizou o registro.
-- MERGE (upsert) é usado para idempotência.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- cnpj.empresas  (~60M linhas)
-- ---------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables t
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'cnpj' AND t.name = 'empresas')
BEGIN
    CREATE TABLE cnpj.empresas (
        cnpj_basico                 VARCHAR(8)    NOT NULL,
        razao_social                VARCHAR(150)  NOT NULL,
        natureza_juridica           VARCHAR(4)    NOT NULL,
        qualificacao_responsavel    VARCHAR(2)    NOT NULL,
        capital_social              DECIMAL(18,2) NULL,
        porte_empresa               VARCHAR(2)    NULL,
        ente_federativo_responsavel VARCHAR(50)   NULL,
        snapshot_date               DATE          NOT NULL,
        data_carga                  DATETIME      NOT NULL DEFAULT GETDATE(),
        CONSTRAINT pk_empresas PRIMARY KEY CLUSTERED (cnpj_basico)
            WITH (FILLFACTOR = 90, DATA_COMPRESSION = PAGE)
    );

    CREATE NONCLUSTERED INDEX ix_empresas_natureza
        ON cnpj.empresas (natureza_juridica)
        WITH (FILLFACTOR = 90, DATA_COMPRESSION = ROW);

    CREATE NONCLUSTERED INDEX ix_empresas_porte
        ON cnpj.empresas (porte_empresa)
        INCLUDE (razao_social, cnpj_basico)
        WITH (FILLFACTOR = 90, DATA_COMPRESSION = ROW);

    CREATE NONCLUSTERED INDEX ix_empresas_snapshot
        ON cnpj.empresas (snapshot_date)
        WITH (FILLFACTOR = 90, DATA_COMPRESSION = ROW);
END
GO

-- ---------------------------------------------------------------------------
-- cnpj.estabelecimentos  (~60M linhas)
-- ---------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables t
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'cnpj' AND t.name = 'estabelecimentos')
BEGIN
    CREATE TABLE cnpj.estabelecimentos (
        cnpj_completo               VARCHAR(14)   NOT NULL,
        cnpj_basico                 VARCHAR(8)    NOT NULL,
        cnpj_ordem                  VARCHAR(4)    NOT NULL,
        cnpj_dv                     VARCHAR(2)    NOT NULL,
        identificador_matriz_filial VARCHAR(1)    NULL,
        nome_fantasia               VARCHAR(55)   NULL,
        situacao_cadastral          VARCHAR(2)    NULL,
        data_situacao_cadastral     DATE          NULL,
        motivo_situacao_cadastral   VARCHAR(2)    NULL,
        nome_cidade_exterior        VARCHAR(55)   NULL,
        pais                        VARCHAR(3)    NULL,
        data_inicio_atividade       DATE          NULL,
        cnae_fiscal                 VARCHAR(10)   NULL,
        cnae_fiscal_secundaria      VARCHAR(1000) NULL,
        tipo_logradouro             VARCHAR(20)   NULL,
        logradouro                  VARCHAR(60)   NULL,
        numero                      VARCHAR(6)    NULL,
        complemento                 VARCHAR(156)  NULL,
        bairro                      VARCHAR(50)   NULL,
        cep                         VARCHAR(8)    NULL,
        uf                          VARCHAR(2)    NULL,
        municipio                   VARCHAR(4)    NULL,
        ddd_1                       VARCHAR(4)    NULL,
        telefone_1                  VARCHAR(9)    NULL,
        ddd_2                       VARCHAR(4)    NULL,
        telefone_2                  VARCHAR(9)    NULL,
        ddd_fax                     VARCHAR(4)    NULL,
        fax                         VARCHAR(9)    NULL,
        correio_eletronico          VARCHAR(115)  NULL,
        situacao_especial           VARCHAR(23)   NULL,
        data_situacao_especial      DATE          NULL,
        snapshot_date               DATE          NOT NULL,
        data_carga                  DATETIME      NOT NULL DEFAULT GETDATE(),
        CONSTRAINT pk_estabelecimentos PRIMARY KEY CLUSTERED (cnpj_completo)
            WITH (FILLFACTOR = 90, DATA_COMPRESSION = PAGE)
    );

    -- Consultas por empresa (join com cnpj.empresas)
    CREATE NONCLUSTERED INDEX ix_estab_cnpj_basico
        ON cnpj.estabelecimentos (cnpj_basico)
        INCLUDE (situacao_cadastral, cnae_fiscal, uf, municipio)
        WITH (FILLFACTOR = 90, DATA_COMPRESSION = ROW);

    -- Filtros operacionais mais comuns
    CREATE NONCLUSTERED INDEX ix_estab_situacao_uf
        ON cnpj.estabelecimentos (situacao_cadastral, uf)
        INCLUDE (cnpj_basico, cnpj_completo, nome_fantasia)
        WITH (FILLFACTOR = 90, DATA_COMPRESSION = ROW);

    CREATE NONCLUSTERED INDEX ix_estab_municipio
        ON cnpj.estabelecimentos (municipio)
        INCLUDE (cnpj_basico, situacao_cadastral)
        WITH (FILLFACTOR = 90, DATA_COMPRESSION = ROW);

    CREATE NONCLUSTERED INDEX ix_estab_cnae
        ON cnpj.estabelecimentos (cnae_fiscal)
        INCLUDE (cnpj_basico, situacao_cadastral, uf)
        WITH (FILLFACTOR = 90, DATA_COMPRESSION = ROW);

    CREATE NONCLUSTERED INDEX ix_estab_snapshot
        ON cnpj.estabelecimentos (snapshot_date)
        WITH (FILLFACTOR = 90, DATA_COMPRESSION = ROW);
END
GO

-- ---------------------------------------------------------------------------
-- cnpj.socios
-- Sem chave natural única confiável na RF → IDENTITY + reprocessamento
-- completo a cada snapshot (truncate + reload dentro de transação).
-- ---------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables t
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'cnpj' AND t.name = 'socios')
BEGIN
    CREATE TABLE cnpj.socios (
        id_socio                        BIGINT       IDENTITY(1,1) NOT NULL,
        cnpj_basico                     VARCHAR(8)   NOT NULL,
        identificador_socio             VARCHAR(1)   NOT NULL,
        nome_socio_razao_social         VARCHAR(150) NOT NULL,
        cnpj_cpf_socio                  VARCHAR(14)  NULL,
        qualificacao_socio              VARCHAR(2)   NOT NULL,
        data_entrada_sociedade          DATE         NULL,
        pais                            VARCHAR(3)   NULL,
        representante_legal             VARCHAR(11)  NULL,
        nome_representante              VARCHAR(60)  NULL,
        qualificacao_representante_legal VARCHAR(2)  NULL,
        faixa_etaria                    VARCHAR(1)   NULL,
        snapshot_date                   DATE         NOT NULL,
        data_carga                      DATETIME     NOT NULL DEFAULT GETDATE(),
        CONSTRAINT pk_socios PRIMARY KEY CLUSTERED (id_socio)
            WITH (FILLFACTOR = 90, DATA_COMPRESSION = PAGE)
    );

    CREATE NONCLUSTERED INDEX ix_socios_cnpj_basico
        ON cnpj.socios (cnpj_basico)
        INCLUDE (nome_socio_razao_social, qualificacao_socio)
        WITH (FILLFACTOR = 90, DATA_COMPRESSION = ROW);

    CREATE NONCLUSTERED INDEX ix_socios_snapshot
        ON cnpj.socios (snapshot_date)
        WITH (FILLFACTOR = 90, DATA_COMPRESSION = ROW);
END
GO

-- ---------------------------------------------------------------------------
-- cnpj.simples  (~10M linhas)
-- ---------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables t
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'cnpj' AND t.name = 'simples')
BEGIN
    CREATE TABLE cnpj.simples (
        cnpj_basico         VARCHAR(8) NOT NULL,
        opcao_pelo_simples  VARCHAR(1) NULL,
        data_opcao_simples  DATE       NULL,
        data_exclusao_simples DATE     NULL,
        opcao_mei           VARCHAR(1) NULL,
        data_opcao_mei      DATE       NULL,
        data_exclusao_mei   DATE       NULL,
        snapshot_date       DATE       NOT NULL,
        data_carga          DATETIME   NOT NULL DEFAULT GETDATE(),
        CONSTRAINT pk_simples PRIMARY KEY CLUSTERED (cnpj_basico)
            WITH (FILLFACTOR = 90, DATA_COMPRESSION = PAGE)
    );

    CREATE NONCLUSTERED INDEX ix_simples_simples
        ON cnpj.simples (opcao_pelo_simples)
        INCLUDE (cnpj_basico)
        WITH (FILLFACTOR = 90, DATA_COMPRESSION = ROW);

    CREATE NONCLUSTERED INDEX ix_simples_mei
        ON cnpj.simples (opcao_mei)
        INCLUDE (cnpj_basico)
        WITH (FILLFACTOR = 90, DATA_COMPRESSION = ROW);

    CREATE NONCLUSTERED INDEX ix_simples_snapshot
        ON cnpj.simples (snapshot_date)
        WITH (FILLFACTOR = 90, DATA_COMPRESSION = ROW);
END
GO

-- =============================================================================
-- TABELAS DE CONTROLE / AUDITORIA
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.tables t
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'cnpj' AND t.name = 'controle_sincronizacao')
BEGIN
    CREATE TABLE cnpj.controle_sincronizacao (
        id_execucao          BIGINT       IDENTITY(1,1) NOT NULL,
        snapshot_date        DATE         NOT NULL,
        status               VARCHAR(20)  NOT NULL
            CONSTRAINT chk_ctrl_status CHECK (
                status IN ('EM_EXECUCAO', 'SUCESSO', 'FALHA', 'CANCELADO')
            ),
        data_inicio_execucao DATETIME     NOT NULL DEFAULT GETDATE(),
        data_fim_execucao    DATETIME     NULL,
        total_arquivos       INT          NULL,
        arquivos_processados INT          NULL,
        arquivos_falha       INT          NULL,
        total_registros      BIGINT       NULL,
        duracao_segundos     INT          NULL,
        erro_mensagem        NVARCHAR(MAX) NULL,
        CONSTRAINT pk_controle_sync PRIMARY KEY CLUSTERED (id_execucao)
    );

    -- Garante que nunca haja dois EM_EXECUCAO para o mesmo snapshot
    CREATE UNIQUE NONCLUSTERED INDEX uix_ctrl_snap_execucao
        ON cnpj.controle_sincronizacao (snapshot_date, status)
        WHERE status = 'EM_EXECUCAO';

    CREATE NONCLUSTERED INDEX ix_ctrl_snapshot
        ON cnpj.controle_sincronizacao (snapshot_date, status)
        INCLUDE (id_execucao, data_inicio_execucao);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables t
               JOIN sys.schemas s ON t.schema_id = s.schema_id
               WHERE s.name = 'cnpj' AND t.name = 'controle_arquivos')
BEGIN
    CREATE TABLE cnpj.controle_arquivos (
        id_arquivo         BIGINT        IDENTITY(1,1) NOT NULL,
        id_execucao        BIGINT        NOT NULL,
        grupo_arquivo      VARCHAR(50)   NOT NULL,
        nome_arquivo       VARCHAR(255)  NOT NULL,
        status             VARCHAR(20)   NOT NULL
            CONSTRAINT chk_arq_status CHECK (
                status IN ('PENDENTE', 'DOWNLOAD', 'EXTRACAO', 'PROCESSAMENTO', 'SUCESSO', 'FALHA')
            ),
        data_inicio        DATETIME      NULL,
        data_fim           DATETIME      NULL,
        total_registros    BIGINT        NULL,
        registros_invalidos BIGINT       NULL,
        erro_mensagem      NVARCHAR(MAX) NULL,
        CONSTRAINT pk_controle_arquivos PRIMARY KEY CLUSTERED (id_arquivo),
        CONSTRAINT fk_arq_execucao FOREIGN KEY (id_execucao)
            REFERENCES cnpj.controle_sincronizacao (id_execucao)
    );

    CREATE NONCLUSTERED INDEX ix_arq_execucao
        ON cnpj.controle_arquivos (id_execucao, status);
END
GO

PRINT '=== SCHEMA DE PRODUCAO CRIADO/VERIFICADO COM SUCESSO ===';
GO
