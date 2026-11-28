"""Publica a apuração e a votação nominal para o dashboard eleitoral."""

import os
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

CHAVE_APURACAO = ("id_turno", "id_secao")
CHAVE_VOTACAO = (*CHAVE_APURACAO, "sq_candidato")
MEDIDAS_APURACAO = (
    "qt_aptos", "qt_comparecimento", "qt_abstencoes",
    "qt_votos_brancos", "qt_votos_nulos", "qt_votos_validos",
)
CHAVES_DIMENSOES = {
    "turno": ("id_turno",),
    "secao": ("id_secao", "id_local_votacao"),
    "candidato": ("sq_candidato",),
    "partido": ("nr_partido",),
    "cargo": ("cd_cargo",),
}
FONTES = (
    "eleicao", "turno", "secao", "zona", "municipio", "uf", "local_votacao",
    "apuracao_secao", "votacao_secao", "candidato", "partido", "cargo",
)


def _dimensoes(silver: dict[str, DataFrame]) -> dict[str, DataFrame]:
    turno = silver["turno"].join(silver["eleicao"], "id_eleicao").select(
        "id_turno", "nr_turno", "cd_eleicao_tse", "cd_pleito", "dt_pleito",
        "id_eleicao", "ano_eleicao", "cd_tipo_eleicao", "nm_tipo_eleicao",
        "ds_eleicao", "tp_abrangencia",
    )

    locais = silver["apuracao_secao"].select("id_secao", "id_local_votacao").distinct()
    secao = (
        locais.join(silver["secao"], "id_secao")
        .join(silver["zona"].select("id_zona", "nr_zona"), "id_zona")
        .join(silver["municipio"], "cd_municipio")
        .join(silver["uf"], "sg_uf")
        .join(
            silver["local_votacao"].select(
                "id_local_votacao", "nr_local_votacao", "nm_local_votacao", "ds_endereco",
            ),
            "id_local_votacao",
        )
        .select(
            "id_secao", "nr_secao", "id_zona", "nr_zona", "cd_municipio",
            "nm_municipio", "sg_uf", "nm_uf", "nm_regiao", "id_local_votacao",
            "nr_local_votacao", "nm_local_votacao", "ds_endereco",
        )
    )
    return {
        "turno": turno,
        "secao": secao,
        "candidato": silver["candidato"].select(
            "sq_candidato", "nr_candidato", "nm_candidato", "nm_urna_candidato",
            "tp_agremiacao", "nm_coligacao", "ds_composicao_coligacao",
            "ds_situacao_julgamento", "ds_sit_tot_turno",
        ),
        "partido": silver["partido"].select(
            "nr_partido", "sg_partido", "nm_partido", "nm_federacao",
        ),
        "cargo": silver["cargo"].select("cd_cargo", "ds_cargo"),
    }


def _gravar_dimensao(
    spark: SparkSession, origem: DataFrame, caminho: str, nome: str, instante: datetime,
) -> DataFrame:
    chaves = CHAVES_DIMENSOES[nome]
    sk = f"sk_{nome}"
    novas = (
        origem.withColumn("vigencia_inicio", F.lit(instante))
        .withColumn("vigencia_fim", F.lit(None).cast("timestamp"))
        .withColumn("registro_atual", F.lit(True))
        .withColumn(sk, F.sha2(F.to_json(F.struct(*chaves, "vigencia_inicio")), 256))
    )

    if not DeltaTable.isDeltaTable(spark, caminho):
        novas.write.format("delta").save(caminho)
    else:
        delta = DeltaTable.forPath(spark, caminho)
        atuais = delta.toDF().filter("registro_atual")
        atributos = [coluna for coluna in origem.columns if coluna not in chaves]
        iguais = F.lit(True)
        for coluna in atributos:
            iguais = iguais & F.col(f"nova.{coluna}").eqNullSafe(F.col(f"atual.{coluna}"))

        alteradas = (
            novas.alias("nova").join(atuais.alias("atual"), list(chaves))
            .filter(~iguais)
            .select("nova.*")
        )
        # A segunda cópia das linhas alteradas força a inserção da nova versão
        # no mesmo MERGE que encerra a versão anterior.
        preparadas = novas.withColumn("_inserir_versao", F.lit(False)).unionByName(
            alteradas.withColumn("_inserir_versao", F.lit(True))
        )
        correspondencia = F.col("atual.registro_atual") & ~F.col("nova._inserir_versao")
        for chave in chaves:
            correspondencia = correspondencia & (F.col(f"atual.{chave}") == F.col(f"nova.{chave}"))
        (
            delta.alias("atual").merge(preparadas.alias("nova"), correspondencia)
            .whenMatchedUpdate(
                condition=~iguais,
                set={"registro_atual": "false", "vigencia_fim": "nova.vigencia_inicio"},
            )
            .whenNotMatchedInsert(values={coluna: f"nova.{coluna}" for coluna in novas.columns})
            .execute()
        )

    return spark.read.format("delta").load(caminho).filter("registro_atual")


def _vincular_dimensoes(
    fato: DataFrame, dimensoes: dict[str, DataFrame], nome_fato: str,
) -> DataFrame:
    for nome, dimensao in dimensoes.items():
        chaves = CHAVES_DIMENSOES[nome]
        fato = fato.join(dimensao.select(*chaves, f"sk_{nome}"), list(chaves), "left")

    chaves_ausentes = " OR ".join(f"sk_{nome} IS NULL" for nome in dimensoes)
    if fato.filter(chaves_ausentes).take(1):
        raise ValueError(f"{nome_fato} contém registros sem correspondência nas dimensões Gold.")
    return fato


def _fato_apuracao(silver: dict[str, DataFrame], dimensoes: dict[str, DataFrame]) -> DataFrame:
    apuracao = silver["apuracao_secao"].withColumn("qt_votos_validos", F.col("qt_votos_nominais"))
    dimensoes_apuracao = {nome: dimensoes[nome] for nome in ("turno", "secao")}
    return _vincular_dimensoes(apuracao, dimensoes_apuracao, "fato_apuracao_secao").select(
        *CHAVE_APURACAO, "id_local_votacao", "sk_turno", "sk_secao", *MEDIDAS_APURACAO, "dt_carga",
    )


def _fato_votacao(silver: dict[str, DataFrame], dimensoes: dict[str, DataFrame]) -> DataFrame:
    fato = (
        silver["votacao_secao"].select(*CHAVE_VOTACAO, "qt_votos", "dt_carga")
        .join(
            silver["apuracao_secao"].select(*CHAVE_APURACAO, "id_local_votacao"),
            list(CHAVE_APURACAO),
            "left",
        )
        .join(
            silver["candidato"].select("sq_candidato", "nr_partido", "cd_cargo"),
            "sq_candidato",
            "left",
        )
    )
    return _vincular_dimensoes(fato, dimensoes, "fato_votacao_secao").select(
        *CHAVE_VOTACAO, "id_local_votacao", *(f"sk_{nome}" for nome in dimensoes),
        "qt_votos", "dt_carga",
    )


def _gravar_fato(
    spark: SparkSession, fato: DataFrame, caminho: str,
    chaves: tuple[str, ...], medidas: tuple[str, ...],
) -> None:
    if not DeltaTable.isDeltaTable(spark, caminho):
        fato.write.format("delta").save(caminho)
        return

    delta = DeltaTable.forPath(spark, caminho)
    correspondencia = " AND ".join(f"atual.{chave} = nova.{chave}" for chave in chaves)
    colunas_atualizaveis = (*medidas, "id_local_votacao", "dt_carga")
    alteracoes = " OR ".join(
        f"NOT (nova.{coluna} <=> atual.{coluna})" for coluna in colunas_atualizaveis
    )
    (
        delta.alias("atual").merge(fato.alias("nova"), correspondencia)
        .whenMatchedUpdate(
            condition=f"nova.dt_carga >= atual.dt_carga AND ({alteracoes})",
            set={
                **{coluna: f"nova.{coluna}" for coluna in colunas_atualizaveis},
                "sk_secao": (
                    "CASE WHEN atual.id_local_votacao <> nova.id_local_votacao "
                    "THEN nova.sk_secao ELSE atual.sk_secao END"
                ),
            },
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def silver_para_gold() -> dict[str, str]:
    from pipeline.spark import criar_spark

    bucket_silver = os.getenv("MINIO_SILVER_BUCKET", "silver")
    bucket_gold = os.getenv("MINIO_GOLD_BUCKET", "gold")
    spark = criar_spark("silver_para_gold")
    try:
        silver = {
            tabela: spark.read.format("delta").load(f"s3a://{bucket_silver}/{tabela}")
            for tabela in FONTES
        }
        instante = datetime.now(timezone.utc)
        destinos = {
            f"dim_{nome}": f"s3a://{bucket_gold}/dim_{nome}" for nome in CHAVES_DIMENSOES
        }
        dimensoes = {
            nome: _gravar_dimensao(spark, origem, destinos[f"dim_{nome}"], nome, instante)
            for nome, origem in _dimensoes(silver).items()
        }
        fatos = (
            ("fato_apuracao_secao", _fato_apuracao(silver, dimensoes), CHAVE_APURACAO, MEDIDAS_APURACAO),
            ("fato_votacao_secao", _fato_votacao(silver, dimensoes), CHAVE_VOTACAO, ("qt_votos",)),
        )
        for nome, fato, chaves, medidas in fatos:
            destinos[nome] = f"s3a://{bucket_gold}/{nome}"
            _gravar_fato(spark, fato, destinos[nome], chaves, medidas)
        print("Tabelas Gold publicadas:", destinos)
        return destinos
    finally:
        spark.stop()
