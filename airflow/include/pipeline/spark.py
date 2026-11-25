"""Cria a SparkSession com Delta Lake e conector S3A apontando para o MinIO.

Recursos configuráveis pelo .env (útil em PCs com pouca memória):
    SPARK_CORES         (padrão: 2)   -> master local[N]
    SPARK_DRIVER_MEMORY (padrão: 2g)
"""

import os

from pyspark.sql import SparkSession

PACOTES = "io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4"


def criar_spark(nome_app: str) -> SparkSession:
    return (
        SparkSession.builder.appName(nome_app)
        .master(f"local[{os.getenv('SPARK_CORES', '2')}]")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.jars.packages", PACOTES)
        .config("spark.jars.ivy", os.getenv("SPARK_JARS_IVY", "/opt/ivy"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"])
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
