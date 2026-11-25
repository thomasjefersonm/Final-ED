-- =============================================================================
-- Banco de origem (OLTP normalizado) - TSE, Presidente 2026, 1o e 2o turno
-- Issue #3 (modelo) / #4 (DDL versionado, executado pelo docker-entrypoint)
-- Diagrama: origem.dbml
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS tse;
SET search_path TO tse;

-- ----------------------------------------------------------------------------
-- Dominio eleitoral
-- ----------------------------------------------------------------------------

CREATE TABLE eleicao (
    id_eleicao        smallint     PRIMARY KEY,
    ano_eleicao       smallint     NOT NULL,
    cd_tipo_eleicao   smallint     NOT NULL,
    nm_tipo_eleicao   varchar(60)  NOT NULL,
    ds_eleicao        varchar(100) NOT NULL,
    tp_abrangencia    char(1)      NOT NULL,
    CONSTRAINT uq_eleicao UNIQUE (ano_eleicao, ds_eleicao)
);

-- No TSE cada turno tem o proprio CD_ELEICAO (6257 = 1o, 6258 = 2o) e CD_PLEITO.
CREATE TABLE turno (
    id_turno          smallint PRIMARY KEY,
    id_eleicao        smallint NOT NULL REFERENCES eleicao (id_eleicao),
    nr_turno          smallint NOT NULL CHECK (nr_turno IN (1, 2)),
    cd_eleicao_tse    integer  NOT NULL UNIQUE,
    cd_pleito         integer  UNIQUE,
    dt_pleito         date     NOT NULL,
    CONSTRAINT uq_turno UNIQUE (id_eleicao, nr_turno)
);

CREATE TABLE cargo (
    cd_cargo          smallint    PRIMARY KEY,
    ds_cargo          varchar(40) NOT NULL UNIQUE
);

-- ----------------------------------------------------------------------------
-- Geografia eleitoral
-- ----------------------------------------------------------------------------

-- 27 UFs + ZZ (Exterior). nm_uf e nm_regiao nao existem nos CSVs: sao seed.
CREATE TABLE uf (
    sg_uf             char(2)     PRIMARY KEY,
    nm_uf             varchar(30) NOT NULL,
    nm_regiao         varchar(15) NOT NULL
);

-- cd_municipio = codigo do TSE (nao e o do IBGE).
CREATE TABLE municipio (
    cd_municipio      integer     PRIMARY KEY,
    nm_municipio      varchar(60) NOT NULL,
    sg_uf             char(2)     NOT NULL REFERENCES uf (sg_uf)
);

-- A zona pertence a UF e pode cobrir varios municipios (1.639 zonas fazem isso),
-- por isso NAO tem FK para municipio.
CREATE TABLE zona (
    id_zona           integer  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sg_uf             char(2)  NOT NULL REFERENCES uf (sg_uf),
    nr_zona           smallint NOT NULL,
    CONSTRAINT uq_zona UNIQUE (sg_uf, nr_zona)
);

CREATE TABLE local_votacao (
    id_local_votacao  integer      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cd_municipio      integer      NOT NULL REFERENCES municipio (cd_municipio),
    id_zona           integer      NOT NULL REFERENCES zona (id_zona),
    nr_local_votacao  integer      NOT NULL,
    nm_local_votacao  varchar(120) NOT NULL,
    ds_endereco       varchar(120),
    CONSTRAINT uq_local_votacao UNIQUE (cd_municipio, id_zona, nr_local_votacao)
);

-- A secao e unica por (UF, zona, numero) e fica em um unico municipio.
-- Local de votacao, aptos e urna mudam entre turnos => ficam em apuracao_secao.
CREATE TABLE secao (
    id_secao          integer  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id_zona           integer  NOT NULL REFERENCES zona (id_zona),
    nr_secao          smallint NOT NULL,
    cd_municipio      integer  NOT NULL REFERENCES municipio (cd_municipio),
    CONSTRAINT uq_secao UNIQUE (id_zona, nr_secao)
);

-- ----------------------------------------------------------------------------
-- Partidos e candidatos
-- ----------------------------------------------------------------------------

CREATE TABLE partido (
    nr_partido        smallint     PRIMARY KEY,
    sg_partido        varchar(20)  NOT NULL UNIQUE,
    nm_partido        varchar(100) NOT NULL,
    nm_federacao      varchar(100)
);

-- ds_sit_tot_turno guarda a situacao ATUAL do candidato: "2o TURNO" depois do
-- 1o turno e "ELEITO" / "NAO ELEITO" depois do 2o. Esse UPDATE (com
-- dt_atualizacao) e o que dispara o SCD2 da dim_candidato na Gold (#13).
CREATE TABLE candidato (
    sq_candidato            bigint       PRIMARY KEY,
    nr_candidato            smallint     NOT NULL,
    nm_candidato            varchar(100) NOT NULL,
    nm_urna_candidato       varchar(60),
    cd_cargo                smallint     NOT NULL REFERENCES cargo (cd_cargo),
    nr_partido              smallint     NOT NULL REFERENCES partido (nr_partido),
    tp_agremiacao           varchar(20),
    nm_coligacao            varchar(100),
    ds_composicao_coligacao varchar(300),
    ds_situacao_julgamento  varchar(40),
    ds_sit_tot_turno        varchar(40),
    dt_atualizacao          timestamp    NOT NULL DEFAULT now(),
    CONSTRAINT uq_candidato UNIQUE (cd_cargo, nr_candidato)
);

-- ----------------------------------------------------------------------------
-- Apuracao e votacao por secao
-- ----------------------------------------------------------------------------

-- 1 linha por secao por turno (fonte: detalhe_votacao_secao).
-- Brancos e nulos ficam AQUI, e nao em votacao_secao.
CREATE TABLE apuracao_secao (
    id_turno                    smallint  NOT NULL REFERENCES turno (id_turno),
    id_secao                    integer   NOT NULL REFERENCES secao (id_secao),
    id_local_votacao            integer   NOT NULL REFERENCES local_votacao (id_local_votacao),
    ds_modelo_urna              varchar(20),
    ds_origem_voto              varchar(100) NOT NULL,
    qt_aptos                    integer   NOT NULL CHECK (qt_aptos >= 0),
    qt_comparecimento           integer   NOT NULL CHECK (qt_comparecimento >= 0),
    qt_abstencoes               integer   NOT NULL CHECK (qt_abstencoes >= 0),
    qt_votos_nominais           integer   NOT NULL CHECK (qt_votos_nominais >= 0),
    qt_votos_brancos            integer   NOT NULL CHECK (qt_votos_brancos >= 0),
    qt_votos_nulos              integer   NOT NULL CHECK (qt_votos_nulos >= 0),
    qt_votos_legenda            integer   NOT NULL DEFAULT 0,
    qt_votos_anulados_apu_sep   integer   NOT NULL DEFAULT 0,
    st_secao_instalada          boolean   NOT NULL,
    st_secao_anulada            boolean   NOT NULL,
    dt_recebimento_bu           timestamp,
    dt_primeira_totalizacao     timestamp,
    dt_carga                    timestamp NOT NULL DEFAULT now(),
    PRIMARY KEY (id_turno, id_secao),
    -- 41 secoes por turno nao foram instaladas: tem aptos, mas 0 comparecimento e 0 abstencao.
    CONSTRAINT ck_apuracao_aptos CHECK (
        NOT st_secao_instalada OR qt_comparecimento + qt_abstencoes = qt_aptos),
    CONSTRAINT ck_apuracao_votos CHECK (
        qt_votos_nominais + qt_votos_brancos + qt_votos_nulos
        + qt_votos_legenda + qt_votos_anulados_apu_sep = qt_comparecimento)
);

-- 1 linha por secao, turno e candidato (fonte: votacao_secao, so votos nominais).
CREATE TABLE votacao_secao (
    id_turno          smallint  NOT NULL,
    id_secao          integer   NOT NULL,
    sq_candidato      bigint    NOT NULL REFERENCES candidato (sq_candidato),
    qt_votos          integer   NOT NULL CHECK (qt_votos >= 0),
    dt_carga          timestamp NOT NULL DEFAULT now(),
    PRIMARY KEY (id_turno, id_secao, sq_candidato),
    FOREIGN KEY (id_turno, id_secao) REFERENCES apuracao_secao (id_turno, id_secao)
);

-- ----------------------------------------------------------------------------
-- Historico de totalizacao (evolucao da apuracao na noite da eleicao)
-- ----------------------------------------------------------------------------

-- Cada ID_TOTALIZACAO do TSE cobre exatamente um par municipio/zona.
-- Percentuais (PE_*) e totais Brasil sao derivados e ficaram de fora.
CREATE TABLE totalizacao (
    id_totalizacao              bigint    PRIMARY KEY,
    id_turno                    smallint  NOT NULL REFERENCES turno (id_turno),
    cd_municipio                integer   NOT NULL REFERENCES municipio (cd_municipio),
    id_zona                     integer   NOT NULL REFERENCES zona (id_zona),
    dt_totalizacao              timestamp NOT NULL,
    qt_secoes_tot               integer   NOT NULL,
    qt_secoes_tot_acumulado     integer   NOT NULL,
    qt_aptos_tot                integer   NOT NULL,
    qt_aptos_tot_acumulado      integer   NOT NULL,
    qt_votos_brancos            integer   NOT NULL,
    qt_votos_brancos_acumulado  integer   NOT NULL,
    qt_votos_nulos              integer   NOT NULL,
    qt_votos_nulos_acumulado    integer   NOT NULL,
    dt_carga                    timestamp NOT NULL DEFAULT now()
);

CREATE TABLE totalizacao_candidato (
    id_totalizacao      bigint  NOT NULL REFERENCES totalizacao (id_totalizacao),
    sq_candidato        bigint  NOT NULL REFERENCES candidato (sq_candidato),
    qt_votos            integer NOT NULL,
    qt_votos_acumulado  integer NOT NULL,
    PRIMARY KEY (id_totalizacao, sq_candidato)
);

-- ----------------------------------------------------------------------------
-- Indices de apoio (FKs e extracao incremental por data)
-- ----------------------------------------------------------------------------

CREATE INDEX ix_municipio_uf            ON municipio (sg_uf);
CREATE INDEX ix_secao_municipio         ON secao (cd_municipio);
CREATE INDEX ix_local_votacao_zona      ON local_votacao (id_zona);
CREATE INDEX ix_apuracao_secao_secao    ON apuracao_secao (id_secao);
CREATE INDEX ix_apuracao_secao_carga    ON apuracao_secao (dt_carga);
CREATE INDEX ix_votacao_secao_candidato ON votacao_secao (sq_candidato);
CREATE INDEX ix_votacao_secao_carga     ON votacao_secao (dt_carga);
CREATE INDEX ix_totalizacao_turno_zona  ON totalizacao (id_turno, cd_municipio, id_zona, dt_totalizacao);
CREATE INDEX ix_totalizacao_carga       ON totalizacao (dt_carga);
CREATE INDEX ix_totcand_candidato       ON totalizacao_candidato (sq_candidato);
