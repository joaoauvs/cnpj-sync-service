-- =============================================================================
-- Schema de PRODUÇÃO — CNPJ Sync Service
-- PostgreSQL 14+
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS cnpj;

-- =============================================================================
-- TABELAS DE REFERÊNCIA
-- =============================================================================

CREATE TABLE IF NOT EXISTS cnpj.cnaes (
    codigo        VARCHAR(10)  NOT NULL,
    descricao     VARCHAR(255) NOT NULL,
    snapshot_date DATE         NOT NULL,
    data_carga    TIMESTAMP    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_cnaes PRIMARY KEY (codigo)
);

CREATE TABLE IF NOT EXISTS cnpj.motivos (
    codigo        VARCHAR(2)   NOT NULL,
    descricao     VARCHAR(255) NOT NULL,
    snapshot_date DATE         NOT NULL,
    data_carga    TIMESTAMP    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_motivos PRIMARY KEY (codigo)
);

CREATE TABLE IF NOT EXISTS cnpj.municipios (
    codigo        VARCHAR(4)   NOT NULL,
    descricao     VARCHAR(255) NOT NULL,
    snapshot_date DATE         NOT NULL,
    data_carga    TIMESTAMP    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_municipios PRIMARY KEY (codigo)
);

CREATE TABLE IF NOT EXISTS cnpj.naturezas (
    codigo        VARCHAR(4)   NOT NULL,
    descricao     VARCHAR(255) NOT NULL,
    snapshot_date DATE         NOT NULL,
    data_carga    TIMESTAMP    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_naturezas PRIMARY KEY (codigo)
);

CREATE TABLE IF NOT EXISTS cnpj.paises (
    codigo        VARCHAR(3)   NOT NULL,
    descricao     VARCHAR(255) NOT NULL,
    snapshot_date DATE         NOT NULL,
    data_carga    TIMESTAMP    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_paises PRIMARY KEY (codigo)
);

CREATE TABLE IF NOT EXISTS cnpj.qualificacoes (
    codigo        VARCHAR(2)   NOT NULL,
    descricao     VARCHAR(255) NOT NULL,
    snapshot_date DATE         NOT NULL,
    data_carga    TIMESTAMP    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_qualificacoes PRIMARY KEY (codigo)
);

-- =============================================================================
-- TABELAS PRINCIPAIS
-- =============================================================================

CREATE TABLE IF NOT EXISTS cnpj.empresas (
    cnpj_basico                 VARCHAR(8)     NOT NULL,
    razao_social                VARCHAR(150)   NOT NULL,
    natureza_juridica           VARCHAR(4)     NOT NULL,
    qualificacao_responsavel    VARCHAR(2)     NOT NULL,
    capital_social              NUMERIC(18,2)  NULL,
    porte_empresa               VARCHAR(2)     NULL,
    ente_federativo_responsavel VARCHAR(50)    NULL,
    snapshot_date               DATE           NOT NULL,
    data_carga                  TIMESTAMP      NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_empresas PRIMARY KEY (cnpj_basico)
);

CREATE INDEX IF NOT EXISTS ix_empresas_natureza
    ON cnpj.empresas (natureza_juridica);

CREATE INDEX IF NOT EXISTS ix_empresas_porte
    ON cnpj.empresas (porte_empresa)
    INCLUDE (razao_social, cnpj_basico);

CREATE INDEX IF NOT EXISTS ix_empresas_snapshot
    ON cnpj.empresas (snapshot_date);

-- ---------------------------------------------------------------------------
-- cnpj.estabelecimentos
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS cnpj.estabelecimentos (
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
    data_carga                  TIMESTAMP     NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_estabelecimentos PRIMARY KEY (cnpj_completo)
);

CREATE INDEX IF NOT EXISTS ix_estab_cnpj_basico
    ON cnpj.estabelecimentos (cnpj_basico)
    INCLUDE (situacao_cadastral, cnae_fiscal, uf, municipio);

CREATE INDEX IF NOT EXISTS ix_estab_situacao_uf
    ON cnpj.estabelecimentos (situacao_cadastral, uf)
    INCLUDE (cnpj_basico, cnpj_completo, nome_fantasia);

CREATE INDEX IF NOT EXISTS ix_estab_municipio
    ON cnpj.estabelecimentos (municipio)
    INCLUDE (cnpj_basico, situacao_cadastral);

CREATE INDEX IF NOT EXISTS ix_estab_cnae
    ON cnpj.estabelecimentos (cnae_fiscal)
    INCLUDE (cnpj_basico, situacao_cadastral, uf);

CREATE INDEX IF NOT EXISTS ix_estab_snapshot
    ON cnpj.estabelecimentos (snapshot_date);

-- ---------------------------------------------------------------------------
-- cnpj.socios  (sem chave natural única → BIGSERIAL + DELETE+INSERT)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS cnpj.socios (
    id_socio                         BIGSERIAL    NOT NULL,
    cnpj_basico                      VARCHAR(8)   NOT NULL,
    identificador_socio              VARCHAR(1)   NOT NULL,
    nome_socio_razao_social          VARCHAR(150) NOT NULL,
    cnpj_cpf_socio                   VARCHAR(14)  NULL,
    qualificacao_socio               VARCHAR(2)   NOT NULL,
    data_entrada_sociedade           DATE         NULL,
    pais                             VARCHAR(3)   NULL,
    representante_legal              VARCHAR(11)  NULL,
    nome_representante               VARCHAR(60)  NULL,
    qualificacao_representante_legal VARCHAR(2)   NULL,
    faixa_etaria                     VARCHAR(1)   NULL,
    snapshot_date                    DATE         NOT NULL,
    data_carga                       TIMESTAMP    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_socios PRIMARY KEY (id_socio)
);

CREATE INDEX IF NOT EXISTS ix_socios_cnpj_basico
    ON cnpj.socios (cnpj_basico)
    INCLUDE (nome_socio_razao_social, qualificacao_socio);

CREATE INDEX IF NOT EXISTS ix_socios_snapshot
    ON cnpj.socios (snapshot_date);

-- ---------------------------------------------------------------------------
-- cnpj.simples
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS cnpj.simples (
    cnpj_basico           VARCHAR(8) NOT NULL,
    opcao_pelo_simples    VARCHAR(1) NULL,
    data_opcao_simples    DATE       NULL,
    data_exclusao_simples DATE       NULL,
    opcao_mei             VARCHAR(1) NULL,
    data_opcao_mei        DATE       NULL,
    data_exclusao_mei     DATE       NULL,
    snapshot_date         DATE       NOT NULL,
    data_carga            TIMESTAMP  NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_simples PRIMARY KEY (cnpj_basico)
);

CREATE INDEX IF NOT EXISTS ix_simples_simples
    ON cnpj.simples (opcao_pelo_simples)
    INCLUDE (cnpj_basico);

CREATE INDEX IF NOT EXISTS ix_simples_mei
    ON cnpj.simples (opcao_mei)
    INCLUDE (cnpj_basico);

CREATE INDEX IF NOT EXISTS ix_simples_snapshot
    ON cnpj.simples (snapshot_date);

-- =============================================================================
-- TABELAS DE CONTROLE / AUDITORIA
-- =============================================================================

CREATE TABLE IF NOT EXISTS cnpj.controle_sincronizacao (
    id_execucao          BIGSERIAL   NOT NULL,
    snapshot_date        DATE        NOT NULL,
    status               VARCHAR(20) NOT NULL
        CONSTRAINT chk_ctrl_status CHECK (
            status IN ('EM_EXECUCAO', 'SUCESSO', 'FALHA', 'CANCELADO')
        ),
    data_inicio_execucao TIMESTAMP   NOT NULL DEFAULT NOW(),
    data_fim_execucao    TIMESTAMP   NULL,
    total_arquivos       INT         NULL,
    arquivos_processados INT         NULL,
    arquivos_falha       INT         NULL,
    total_registros      BIGINT      NULL,
    duracao_segundos     INT         NULL,
    erro_mensagem        TEXT        NULL,
    CONSTRAINT pk_controle_sync PRIMARY KEY (id_execucao)
);

-- Garante que nunca haja dois EM_EXECUCAO para o mesmo snapshot
CREATE UNIQUE INDEX IF NOT EXISTS uix_ctrl_snap_execucao
    ON cnpj.controle_sincronizacao (snapshot_date)
    WHERE status = 'EM_EXECUCAO';

CREATE INDEX IF NOT EXISTS ix_ctrl_snapshot
    ON cnpj.controle_sincronizacao (snapshot_date, status)
    INCLUDE (id_execucao, data_inicio_execucao);

CREATE TABLE IF NOT EXISTS cnpj.controle_arquivos (
    id_arquivo          BIGSERIAL    NOT NULL,
    id_execucao         BIGINT       NOT NULL,
    grupo_arquivo       VARCHAR(50)  NOT NULL,
    nome_arquivo        VARCHAR(255) NOT NULL,
    status              VARCHAR(20)  NOT NULL
        CONSTRAINT chk_arq_status CHECK (
            status IN ('PENDENTE', 'DOWNLOAD', 'EXTRACAO', 'PROCESSAMENTO', 'SUCESSO', 'FALHA')
        ),
    data_inicio         TIMESTAMP    NULL,
    data_fim            TIMESTAMP    NULL,
    total_registros     BIGINT       NULL,
    registros_invalidos BIGINT       NULL,
    erro_mensagem       TEXT         NULL,
    CONSTRAINT pk_controle_arquivos PRIMARY KEY (id_arquivo),
    CONSTRAINT fk_arq_execucao FOREIGN KEY (id_execucao)
        REFERENCES cnpj.controle_sincronizacao (id_execucao)
);

CREATE INDEX IF NOT EXISTS ix_arq_execucao
    ON cnpj.controle_arquivos (id_execucao, status);
