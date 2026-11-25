"""Ingestão: landing (CSV bruto) -> bronze (Delta Lake).

Para cada tabela da landing:
    s3a://landing/<tabela>/<tabela>_<carimbo>.csv  ->  s3a://bronze/<tabela>  (Delta)

Regras:
- Os dados entram como vieram (todas as colunas como texto); tipagem e
  limpeza ficam para a silver.
- Colunas de auditoria: nome_arquivo (CSV de origem) e data_hora_bronze.
- Tabelas incrementais (as mesmas da extração): cada CSV novo é ACRESCENTADO
  (append). Um CSV só é processado uma vez: os arquivos já carregados são
  lidos da própria coluna nome_arquivo da tabela Delta.
- Tabelas de cadastro: a bronze é SOBRESCRITA com o CSV mais recente
  (cada CSV é uma foto completa da tabela). Se o mais recente já foi carregado,
  nada é feito.
"""

import os

import boto3
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from pipeline.extracao import TABELAS_INCREMENTAIS


def _bucket(camada: str) -> str:
    return os.getenv(f"MINIO_{camada.upper()}_BUCKET", camada)


def listar_arquivos_landing() -> dict[str, list[str]]:
    """{tabela: [arquivos CSV em ordem cronológica]} (ignora _checkpoint)."""
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
    )
    arquivos: dict[str, list[str]] = {}
    paginas = s3.get_paginator("list_objects_v2").paginate(Bucket=_bucket("landing"))
    for pagina in paginas:
        for obj in pagina.get("Contents", []):
            chave = obj["Key"]
            if chave.startswith("_") or not chave.endswith(".csv"):
                continue
            tabela, nome = chave.split("/", 1)
            arquivos.setdefault(tabela, []).append(nome)
    # o carimbo aaaammdd_hhmmss no nome garante a ordem cronológica
    return {t: sorted(nomes) for t, nomes in arquivos.items()}


def _arquivos_ja_carregados(spark, caminho: str) -> set[str]:
    if not DeltaTable.isDeltaTable(spark, caminho):
        return set()
    linhas = spark.read.format("delta").load(caminho).select("nome_arquivo").distinct().collect()
    return {linha[0] for linha in linhas}


def carregar_tabela(spark, tabela: str, arquivos: list[str]) -> dict:
    landing = f"s3a://{_bucket('landing')}/{tabela}"
    destino = f"s3a://{_bucket('bronze')}/{tabela}"
    incremental = tabela in TABELAS_INCREMENTAIS
    ja_carregados = _arquivos_ja_carregados(spark, destino)

    if incremental:
        novos = [a for a in arquivos if a not in ja_carregados]
        modo = "append"
    else:
        novos = [arquivos[-1]] if arquivos[-1] not in ja_carregados else []
        modo = "overwrite"

    if not novos:
        resumo = {"tabela": tabela, "modo": modo, "arquivos": 0, "linhas": 0}
        print(resumo, "(nada novo)")
        return resumo

    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)  # bronze guarda o dado bruto, como texto
        .option("multiLine", True)
        .option("escape", '"')
        .csv([f"{landing}/{a}" for a in novos])
        .withColumn("nome_arquivo", F.element_at(F.split(F.input_file_name(), "/"), -1))
        .withColumn("data_hora_bronze", F.current_timestamp())
    )
    (
        df.write.format("delta")
        .mode(modo)
        .option("overwriteSchema", "true" if modo == "overwrite" else "false")
        .save(destino)
    )
    linhas = df.count() if modo == "append" else spark.read.format("delta").load(destino).count()
    resumo = {"tabela": tabela, "modo": modo, "arquivos": len(novos), "linhas": linhas,
              "destino": destino}
    print(resumo)
    return resumo


def landing_para_bronze() -> list[dict]:
    from pipeline.spark import criar_spark

    spark = criar_spark("landing_para_bronze")
    try:
        resumos = [
            carregar_tabela(spark, tabela, arquivos)
            for tabela, arquivos in sorted(listar_arquivos_landing().items())
        ]
    finally:
        spark.stop()
    return resumos
