-- =========================
-- DIMENSÕES (sem UNIQUE)
-- =========================

-- tipo_acidente + classificacao_acidente
CREATE TABLE dim_acidente (
  id_acidente            SERIAL PRIMARY KEY,
  tipo_acidente          VARCHAR(100) NOT NULL,
  classificacao_acidente VARCHAR(100) NOT NULL
);

-- sentido/tipo/tracado/uso do solo
CREATE TABLE dim_pista (
  id_pista     SERIAL PRIMARY KEY,
  sentido_via  VARCHAR(255)  NOT NULL,
  tipo_pista   VARCHAR(255)  NOT NULL,
  tracado_via  VARCHAR(255)  NOT NULL,
  uso_solo     VARCHAR(255)  NOT NULL
);

ALTER TABLE dim_pista
  ALTER COLUMN sentido_via TYPE VARCHAR(255) USING LEFT(sentido_via,255),
  ALTER COLUMN tipo_pista  TYPE VARCHAR(255) USING LEFT(tipo_pista,255),
  ALTER COLUMN tracado_via TYPE VARCHAR(255) USING LEFT(tracado_via,255),
  ALTER COLUMN uso_solo    TYPE VARCHAR(255) USING LEFT(uso_solo,255);
-- ou TEXT para todas, se quiser eliminar o limite


-- veiculo
CREATE TABLE dim_veiculo (
  id_veiculo    SERIAL PRIMARY KEY,
  tipo_veiculo  VARCHAR(100) NOT NULL,
  marca         VARCHAR(100) NOT NULL,
  ano_fabricacao INTEGER     NOT NULL
);

-- localidade
CREATE TABLE dim_localidade (
  id_localidade SERIAL PRIMARY KEY,
  municipio     VARCHAR(100) NOT NULL,
  uf            VARCHAR(20)  NOT NULL,
  br            INTEGER      NOT NULL,
  km            NUMERIC(10,2) NOT NULL,
  latitude      NUMERIC(10,6) NOT NULL,
  longitude     NUMERIC(10,6) NOT NULL
);

-- vitima/pessoa
CREATE TABLE dim_vitima (
  id_vitima      SERIAL PRIMARY KEY,
  sexo           VARCHAR(20)  NOT NULL,
  idade          INTEGER      NOT NULL,
  estado_fisico  VARCHAR(50)  NOT NULL,
  tipo_envolvido VARCHAR(50)  NOT NULL
);

-- condição meteorológica
CREATE TABLE dim_cnd_meteorologica(
	id_cnd SERIAL PRIMARY KEY,
	cnd_meteorologica varchar(100)
);

-- tempo (data + hora)
CREATE TABLE dim_tempo (
  id_tempo    SERIAL PRIMARY KEY,
  data_completa DATE       NOT NULL,
  horario       TIME,
  fase_dia      VARCHAR(20) NOT NULL,
  ano           INTEGER     NOT NULL,
  mes           INTEGER     NOT NULL,
  dia           INTEGER     NOT NULL,
  trimestre     INTEGER     NOT NULL,
  nome_mes      VARCHAR(20) NOT NULL,
  dia_semana    VARCHAR(20) NOT NULL
);

-- =========================
-- FATO (sem PK composto; permite duplicatas)
-- =========================
CREATE TABLE fato_acidentes (
  id_fato        SERIAL PRIMARY KEY,
  id_tempo       INTEGER NOT NULL,
  id_vitima      INTEGER NOT NULL,
  id_pista       INTEGER NOT NULL,
  id_acidente    INTEGER NOT NULL,
  id_veiculo     INTEGER NOT NULL,
  id_localidade  INTEGER NOT NULL,
  id_cnd         INTEGER NOT NULL,
  ilesos         INTEGER NOT NULL,
  feridos_leves  INTEGER NOT NULL,
  feridos_graves INTEGER NOT NULL,
  mortos         INTEGER NOT NULL,
  CONSTRAINT fk_fato_tempo       FOREIGN KEY (id_tempo)      REFERENCES dim_tempo(id_tempo),
  CONSTRAINT fk_fato_cnd         FOREIGN KEY (id_cnd)        REFERENCES dim_cnd_meteorologica(id_cnd),
  CONSTRAINT fk_fato_vitima      FOREIGN KEY (id_vitima)     REFERENCES dim_vitima(id_vitima),
  CONSTRAINT fk_fato_pista       FOREIGN KEY (id_pista)      REFERENCES dim_pista(id_pista),
  CONSTRAINT fk_fato_acidente    FOREIGN KEY (id_acidente)   REFERENCES dim_acidente(id_acidente),
  CONSTRAINT fk_fato_veiculo     FOREIGN KEY (id_veiculo)    REFERENCES dim_veiculo(id_veiculo),
  CONSTRAINT fk_fato_localidade  FOREIGN KEY (id_localidade) REFERENCES dim_localidade(id_localidade)
);

-- =========================
-- ÍNDICES úteis (FKs)
-- =========================
CREATE INDEX idx_fato_id_tempo       ON fato_acidentes (id_tempo);
CREATE INDEX idx_fato_id_vitima      ON fato_acidentes (id_vitima);
CREATE INDEX idx_fato_id_pista       ON fato_acidentes (id_pista);
CREATE INDEX idx_fato_id_acidente    ON fato_acidentes (id_acidente);
CREATE INDEX idx_fato_id_veiculo     ON fato_acidentes (id_veiculo);
CREATE INDEX idx_fato_id_localidade  ON fato_acidentes (id_localidade);
CREATE INDEX idx_fato_id_cnd         ON fato_acidentes (id_cnd);

CREATE INDEX IF NOT EXISTS ix_dim_acidente_nat
  ON dim_acidente (tipo_acidente, classificacao_acidente);

CREATE INDEX IF NOT EXISTS ix_dim_pista_nat
  ON dim_pista (sentido_via, tipo_pista, tracado_via, uso_solo);

CREATE INDEX IF NOT EXISTS br
  ON dim_veiculo (tipo_veiculo, marca, ano_fabricacao);

CREATE INDEX IF NOT EXISTS ix_dim_localidade_nat
  ON dim_localidade (municipio, uf, br, km, latitude, longitude);

CREATE INDEX IF NOT EXISTS ix_dim_vitima_nat
  ON dim_vitima (sexo, idade, estado_fisico, tipo_envolvido);

CREATE INDEX IF NOT EXISTS ix_dim_tempo_nat
  ON dim_tempo (data_completa, horario, fase_dia, ano, mes, dia, trimestre, nome_mes, dia_semana);

CREATE INDEX IF NOT EXISTS ix_dim_cnd_nat
  ON dim_cnd_meteorologica (cnd_meteorologica);

-- 1) adicionar a coluna gerada
ALTER TABLE public.fato_acidentes
ADD COLUMN chave_hash CHAR(32)
GENERATED ALWAYS AS (
  md5(
    id_tempo::text      || '|' ||
    id_localidade::text || '|' ||
    id_pista::text      || '|' ||
    id_cnd::text
  )
) STORED;

-- 2) indexar para distinct/counts e joins rápidos
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_fato_chave_hash
  ON public.fato_acidentes (chave_hash);

ALTER TABLE public.dim_tempo
  ADD COLUMN IF NOT EXISTS mes_ord         SMALLINT,
  ADD COLUMN IF NOT EXISTS dia_semana_ord  SMALLINT;

UPDATE public.dim_tempo t
SET
  nome_mes = CASE EXTRACT(MONTH FROM t.data_completa)
               WHEN 1 THEN 'janeiro'     WHEN 2 THEN 'fevereiro' WHEN 3 THEN 'março'
               WHEN 4 THEN 'abril'       WHEN 5 THEN 'maio'      WHEN 6 THEN 'junho'
               WHEN 7 THEN 'julho'       WHEN 8 THEN 'agosto'    WHEN 9 THEN 'setembro'
               WHEN 10 THEN 'outubro'    WHEN 11 THEN 'novembro' WHEN 12 THEN 'dezembro'
             END,
  dia_semana = CASE EXTRACT(ISODOW FROM t.data_completa)  -- 1=Seg ... 7=Dom
                 WHEN 1 THEN 'segunda-feira' WHEN 2 THEN 'terça-feira'  WHEN 3 THEN 'quarta-feira'
                 WHEN 4 THEN 'quinta-feira'  WHEN 5 THEN 'sexta-feira'  WHEN 6 THEN 'sábado'
                 WHEN 7 THEN 'domingo'
               END,
  mes_ord        = EXTRACT(MONTH FROM t.data_completa)::smallint,
  dia_semana_ord = EXTRACT(ISODOW FROM t.data_completa)::smallint;



