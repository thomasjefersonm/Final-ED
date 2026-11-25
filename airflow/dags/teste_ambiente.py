"""DAG de teste do ambiente: prova que o Airflow conversa com o Postgres
de origem, com o MinIO e que o Spark grava/lê Delta Lake no Data Lake.

    [postgres_origem] ──▶ [minio_buckets] ──▶ [spark_delta_minio]

Rodar manualmente (Trigger) — sem agendamento.
"""

import os

import pendulum
from airflow.sdk import dag, task

SPARK_PYTHON = os.getenv("SPARK_VENV", "/opt/spark-venv") + "/bin/python"


@dag(
    dag_id="teste_ambiente",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    tags=["teste", "infra"],
    default_args={"retries": 2, "retry_delay": pendulum.duration(seconds=30)},
    doc_md=__doc__,
)
def teste_ambiente():
    @task.external_python(python=SPARK_PYTHON)
    def postgres_origem() -> dict:
        import os

        import pg8000.dbapi

        con = pg8000.dbapi.connect(
            host=os.environ["PG_HOST"],
            port=int(os.environ["PG_PORT"]),
            database=os.environ["PG_DATABASE"],
            user=os.environ["PG_USER"],
            password=os.environ["PG_PASSWORD"],
            timeout=20,
        )
        cur = con.cursor()
        schema = os.getenv("PG_SCHEMA", "public")
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s AND table_type = 'BASE TABLE' ORDER BY 1",
            (schema,),
        )
        tabelas = [r[0] for r in cur.fetchall()]
        con.close()
        print(f"{len(tabelas)} tabelas: {tabelas}")
        assert len(tabelas) >= 10, "origem precisa ter no mínimo 10 tabelas"
        return {"tabelas": len(tabelas)}

    @task.external_python(python=SPARK_PYTHON)
    def minio_buckets() -> list:
        import os

        import boto3

        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["MINIO_ENDPOINT"],
            aws_access_key_id=os.environ["MINIO_ROOT_USER"],
            aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
            region_name="us-east-1",
        )
        buckets = sorted(b["Name"] for b in s3.list_buckets()["Buckets"])
        print("buckets:", buckets)
        faltando = {"landing", "bronze", "silver", "gold"} - set(buckets)
        assert not faltando, f"buckets faltando: {faltando}"
        return buckets

    @task.external_python(python=SPARK_PYTHON)
    def spark_delta_minio() -> int:
        import os

        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("teste_ambiente")
            .master("local[*]")
            .config("spark.jars.packages",
                    "io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4")
            .config("spark.jars.ivy", os.getenv("SPARK_JARS_IVY", "/opt/ivy"))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"])
            .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
            .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )
        caminho = "s3a://bronze/_smoke_test_delta"
        spark.range(100).write.format("delta").mode("overwrite").save(caminho)
        total = spark.read.format("delta").load(caminho).count()
        print(f"Delta gravado e lido em {caminho}: {total} linhas")

        # limpa o teste
        jvm = spark._jvm
        path = jvm.org.apache.hadoop.fs.Path(caminho)
        path.getFileSystem(spark._jsc.hadoopConfiguration()).delete(path, True)
        spark.stop()
        assert total == 100
        return total

    postgres_origem() >> minio_buckets() >> spark_delta_minio()


teste_ambiente()
