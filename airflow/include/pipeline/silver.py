"""Transformação: bronze (Delta bruto/texto) -> silver (Delta tipado e limpo).

Para cada tabela definida no contrato (01_schema.sql):
    s3a://bronze/<tabela>  ->  s3a://silver/<tabela>  (Delta)

Regras aplicadas:
- Limpeza de texto: trim, colapso de espaços múltiplos e conversão de strings
  vazias ou marcadores nulos do TSE (#NULO#, #NE#) para NULL.
- Padronização: siglas (UF, partido, abrangência) em maiúsculas.
- Tipagem explícita: conversão de todas as colunas para os tipos do schema OLTP
  (smallint, int, bigint, boolean, date, timestamp, string).
- Deduplicação: garante unicidade pela chave primária, preservando o registro
  mais recente (por dt_carga/dt_atualizacao e data_hora_bronze).
- Auditoria: mantém nome_arquivo e data_hora_bronze e adiciona data_hora_silver.
"""

import os

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# contrato de schema, chaves e regras baseado em Banco_normalizado/01_schema.sql
ESPECIFICACOES_TABELAS: dict[str, dict] = {
    "eleicao": {
        "colunas": {
            "id_eleicao": "smallint",
            "ano_eleicao": "smallint",
            "cd_tipo_eleicao": "smallint",
            "nm_tipo_eleicao": "string",
            "ds_eleicao": "string",
            "tp_abrangencia": "string",
        },
        "pk": ["id_eleicao"],
        "not_null": [
            "id_eleicao", "ano_eleicao", "cd_tipo_eleicao",
            "nm_tipo_eleicao", "ds_eleicao", "tp_abrangencia",
        ],
        "unicas": [["ano_eleicao", "ds_eleicao"]],
        "upper": ["tp_abrangencia"],
    },
    "turno": {
        "colunas": {
            "id_turno": "smallint",
            "id_eleicao": "smallint",
            "nr_turno": "smallint",
            "cd_eleicao_tse": "int",
            "cd_pleito": "int",
            "dt_pleito": "date",
        },
        "pk": ["id_turno"],
        "not_null": ["id_turno", "id_eleicao", "nr_turno", "cd_eleicao_tse", "dt_pleito"],
        "unicas": [["cd_eleicao_tse"], ["id_eleicao", "nr_turno"]],
    },
    "cargo": {
        "colunas": {
            "cd_cargo": "smallint",
            "ds_cargo": "string",
        },
        "pk": ["cd_cargo"],
        "not_null": ["cd_cargo", "ds_cargo"],
        "unicas": [["ds_cargo"]],
    },
    "uf": {
        "colunas": {
            "sg_uf": "string",
            "nm_uf": "string",
            "nm_regiao": "string",
        },
        "pk": ["sg_uf"],
        "not_null": ["sg_uf", "nm_uf", "nm_regiao"],
        "upper": ["sg_uf"],
    },
    "municipio": {
        "colunas": {
            "cd_municipio": "int",
            "nm_municipio": "string",
            "sg_uf": "string",
        },
        "pk": ["cd_municipio"],
        "not_null": ["cd_municipio", "nm_municipio", "sg_uf"],
        "upper": ["sg_uf"],
    },
    "zona": {
        "colunas": {
            "id_zona": "int",
            "sg_uf": "string",
            "nr_zona": "smallint",
        },
        "pk": ["id_zona"],
        "not_null": ["id_zona", "sg_uf", "nr_zona"],
        "unicas": [["sg_uf", "nr_zona"]],
        "upper": ["sg_uf"],
    },
    "local_votacao": {
        "colunas": {
            "id_local_votacao": "int",
            "cd_municipio": "int",
            "id_zona": "int",
            "nr_local_votacao": "int",
            "nm_local_votacao": "string",
            "ds_endereco": "string",
        },
        "pk": ["id_local_votacao"],
        "not_null": [
            "id_local_votacao", "cd_municipio", "id_zona",
            "nr_local_votacao", "nm_local_votacao",
        ],
        "unicas": [["cd_municipio", "id_zona", "nr_local_votacao"]],
    },
    "secao": {
        "colunas": {
            "id_secao": "int",
            "id_zona": "int",
            "nr_secao": "smallint",
            "cd_municipio": "int",
        },
        "pk": ["id_secao"],
        "not_null": ["id_secao", "id_zona", "nr_secao", "cd_municipio"],
        "unicas": [["id_zona", "nr_secao"]],
    },
    "partido": {
        "colunas": {
            "nr_partido": "smallint",
            "sg_partido": "string",
            "nm_partido": "string",
            "nm_federacao": "string",
        },
        "pk": ["nr_partido"],
        "not_null": ["nr_partido", "sg_partido", "nm_partido"],
        "unicas": [["sg_partido"]],
        "upper": ["sg_partido"],
    },
    "candidato": {
        "colunas": {
            "sq_candidato": "bigint",
            "nr_candidato": "smallint",
            "nm_candidato": "string",
            "nm_urna_candidato": "string",
            "cd_cargo": "smallint",
            "nr_partido": "smallint",
            "tp_agremiacao": "string",
            "nm_coligacao": "string",
            "ds_composicao_coligacao": "string",
            "ds_situacao_julgamento": "string",
            "ds_sit_tot_turno": "string",
            "dt_atualizacao": "timestamp",
        },
        "pk": ["sq_candidato"],
        "not_null": [
            "sq_candidato", "nr_candidato", "nm_candidato",
            "cd_cargo", "nr_partido", "dt_atualizacao",
        ],
        "unicas": [["cd_cargo", "nr_candidato"]],
        "ordenacao_dedup": "dt_atualizacao",
    },
    "apuracao_secao": {
        "colunas": {
            "id_turno": "smallint",
            "id_secao": "int",
            "id_local_votacao": "int",
            "ds_modelo_urna": "string",
            "ds_origem_voto": "string",
            "qt_aptos": "int",
            "qt_comparecimento": "int",
            "qt_abstencoes": "int",
            "qt_votos_nominais": "int",
            "qt_votos_brancos": "int",
            "qt_votos_nulos": "int",
            "qt_votos_legenda": "int",
            "qt_votos_anulados_apu_sep": "int",
            "st_secao_instalada": "boolean",
            "st_secao_anulada": "boolean",
            "dt_recebimento_bu": "timestamp",
            "dt_primeira_totalizacao": "timestamp",
            "dt_carga": "timestamp",
        },
        "pk": ["id_turno", "id_secao"],
        "not_null": [
            "id_turno", "id_secao", "id_local_votacao", "ds_origem_voto",
            "qt_aptos", "qt_comparecimento", "qt_abstencoes",
            "qt_votos_nominais", "qt_votos_brancos", "qt_votos_nulos",
            "qt_votos_legenda", "qt_votos_anulados_apu_sep",
            "st_secao_instalada", "st_secao_anulada", "dt_carga",
        ],
        "ordenacao_dedup": "dt_carga",
    },
    "votacao_secao": {
        "colunas": {
            "id_turno": "smallint",
            "id_secao": "int",
            "sq_candidato": "bigint",
            "qt_votos": "int",
            "dt_carga": "timestamp",
        },
        "pk": ["id_turno", "id_secao", "sq_candidato"],
        "not_null": ["id_turno", "id_secao", "sq_candidato", "qt_votos", "dt_carga"],
        "ordenacao_dedup": "dt_carga",
    },
    "totalizacao": {
        "colunas": {
            "id_totalizacao": "bigint",
            "id_turno": "smallint",
            "cd_municipio": "int",
            "id_zona": "int",
            "dt_totalizacao": "timestamp",
            "qt_secoes_tot": "int",
            "qt_secoes_tot_acumulado": "int",
            "qt_aptos_tot": "int",
            "qt_aptos_tot_acumulado": "int",
            "qt_votos_brancos": "int",
            "qt_votos_brancos_acumulado": "int",
            "qt_votos_nulos": "int",
            "qt_votos_nulos_acumulado": "int",
            "dt_carga": "timestamp",
        },
        "pk": ["id_totalizacao"],
        "not_null": [
            "id_totalizacao", "id_turno", "cd_municipio", "id_zona", "dt_totalizacao",
            "qt_secoes_tot", "qt_secoes_tot_acumulado",
            "qt_aptos_tot", "qt_aptos_tot_acumulado",
            "qt_votos_brancos", "qt_votos_brancos_acumulado",
            "qt_votos_nulos", "qt_votos_nulos_acumulado",
            "dt_carga",
        ],
        "ordenacao_dedup": "dt_carga",
    },
    "totalizacao_candidato": {
        "colunas": {
            "id_totalizacao": "bigint",
            "sq_candidato": "bigint",
            "qt_votos": "int",
            "qt_votos_acumulado": "int",
        },
        "pk": ["id_totalizacao", "sq_candidato"],
        "not_null": ["id_totalizacao", "sq_candidato", "qt_votos", "qt_votos_acumulado"],
    },
}

_SENTINELAS_NULO = {"", "#NULO#", "#NE#", "NULL", "NONE"}
_VALORES_TRUE = ["t", "true", "1", "s", "sim"]
_VALORES_FALSE = ["f", "false", "0", "n", "nao", "não"]


def _bucket(camada: str) -> str:
    return os.getenv(f"MINIO_{camada.upper()}_BUCKET", camada)


def _limpar_texto(col: Column) -> Column:
    """Remove espaços extras e converte vazios/sentinelas do TSE para NULL."""
    limpo = F.trim(F.regexp_replace(col.cast("string"), r"\s+", " "))
    return F.when(F.upper(limpo).isin(list(_SENTINELAS_NULO)), F.lit(None)).otherwise(limpo)


def _converter_booleano(col: Column) -> Column:
    """Converte representações textuais comuns do Postgres/CSV para boolean."""
    norm = F.lower(_limpar_texto(col))
    return (
        F.when(norm.isin(_VALORES_TRUE), F.lit(True))
        .when(norm.isin(_VALORES_FALSE), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def transformar_dataframe(df: DataFrame, spec: dict) -> DataFrame:
    """Aplica limpeza, tipagem, padronização e deduplicação em um DataFrame da Bronze."""
    colunas_upper = set(spec.get("upper", []))
    selecionadas = []

    for nome_col, tipo in spec["colunas"].items():
        if nome_col not in df.columns:
            raise ValueError(f"Coluna esperada '{nome_col}' não encontrada na Bronze.")

        col_limpa = _limpar_texto(F.col(nome_col))
        if tipo == "string":
            expr = F.upper(col_limpa) if nome_col in colunas_upper else col_limpa
        elif tipo == "boolean":
            expr = _converter_booleano(F.col(nome_col))
        else:
            expr = col_limpa.cast(tipo)

        selecionadas.append(expr.alias(nome_col))

    # preserva colunas de auditoria da Bronze para linhagem
    for col_aud in ("nome_arquivo", "data_hora_bronze"):
        if col_aud in df.columns:
            selecionadas.append(F.col(col_aud))

    df_tratado = df.select(*selecionadas)

    # deduplicação pela PK mantendo o registro mais recente
    criterios_ordem = []
    if col_ord := spec.get("ordenacao_dedup"):
        criterios_ordem.append(F.col(col_ord).desc_nulls_last())
    if "data_hora_bronze" in df_tratado.columns:
        criterios_ordem.append(F.col("data_hora_bronze").desc_nulls_last())
    if "nome_arquivo" in df_tratado.columns:
        criterios_ordem.append(F.col("nome_arquivo").desc_nulls_last())

    if criterios_ordem:
        janela = Window.partitionBy(*spec["pk"]).orderBy(*criterios_ordem)
        df_tratado = (
            df_tratado.withColumn("_rn", F.row_number().over(janela))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
    else:
        df_tratado = df_tratado.dropDuplicates(spec["pk"])

    return df_tratado.withColumn("data_hora_silver", F.current_timestamp())


def processar_tabela(spark, tabela: str, spec: dict) -> dict:
    origem = f"s3a://{_bucket('bronze')}/{tabela}"
    destino = f"s3a://{_bucket('silver')}/{tabela}"

    if not DeltaTable.isDeltaTable(spark, origem):
        resumo = {"tabela": tabela, "status": "ignorada", "motivo": "tabela ausente na bronze"}
        print(resumo)
        return resumo

    df_bronze = spark.read.format("delta").load(origem)
    linhas_bronze = df_bronze.count()

    df_silver = transformar_dataframe(df_bronze, spec)
    (
        df_silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(destino)
    )

    linhas_silver = spark.read.format("delta").load(destino).count()
    resumo = {
        "tabela": tabela,
        "status": "ok",
        "linhas_bronze": linhas_bronze,
        "linhas_silver": linhas_silver,
        "duplicadas_removidas": linhas_bronze - linhas_silver,
        "destino": destino,
    }
    print(resumo)
    return resumo


def bronze_para_silver() -> list[dict]:
    from pipeline.spark import criar_spark

    spark = criar_spark("bronze_para_silver")
    try:
        resumos = [
            processar_tabela(spark, tabela, spec)
            for tabela, spec in ESPECIFICACOES_TABELAS.items()
        ]
    finally:
        spark.stop()
    return resumos