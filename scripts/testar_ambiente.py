"""Smoke test do ambiente local da arquitetura medalhão.

Valida, sem alterar nada de forma permanente:
  1. MinIO: conexão, existência dos buckets landing/bronze/silver/gold e
     escrita/leitura/remoção de um objeto de teste em cada bucket;
  2. PostgreSQL de origem: conexão e contagem de linhas de cada tabela.

Uso:  uv run task testar
"""

import os
import sys
from datetime import datetime, timezone

import boto3
import pg8000.dbapi
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv()

CAMADAS = ["landing", "bronze", "silver", "gold"]


def testar_minio() -> bool:
    print("== MinIO ==")
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9020"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    existentes = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    ok = True
    for camada in CAMADAS:
        bucket = os.getenv(f"MINIO_{camada.upper()}_BUCKET", camada)
        if bucket not in existentes:
            print(f"  [FALHA] bucket '{bucket}' não existe")
            ok = False
            continue
        chave = "_smoke_test/teste.txt"
        conteudo = f"smoke test {datetime.now(timezone.utc).isoformat()}".encode()
        s3.put_object(Bucket=bucket, Key=chave, Body=conteudo)
        lido = s3.get_object(Bucket=bucket, Key=chave)["Body"].read()
        s3.delete_object(Bucket=bucket, Key=chave)
        status = "OK" if lido == conteudo else "FALHA"
        ok &= status == "OK"
        print(f"  [{status}] {bucket}: escrita/leitura/remoção")
    return ok


def testar_postgres() -> bool:
    print("== PostgreSQL de origem ==")
    if not os.getenv("PG_HOST") or not os.getenv("PG_PASSWORD"):
        print("  [PULADO] preencha PG_HOST/PG_USER/PG_PASSWORD no .env")
        return True
    schema = os.getenv("PG_SCHEMA", "public")
    conexao = pg8000.dbapi.connect(
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT", "5432")),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        timeout=20,
    )
    cursor = conexao.cursor()
    cursor.execute(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        (schema,),
    )
    tabelas = [linha[0] for linha in cursor.fetchall()]
    for tabela in tabelas:
        cursor.execute(f'SELECT count(*) FROM "{schema}"."{tabela}"')
        print(f"  {tabela:<25} {cursor.fetchone()[0]:>12,} linhas")
    cursor.close()
    conexao.close()
    print(f"  [OK] {len(tabelas)} tabelas no schema '{schema}'")
    return len(tabelas) > 0


if __name__ == "__main__":
    resultado = testar_minio() & testar_postgres()
    print("\nAmbiente OK" if resultado else "\nAmbiente com FALHAS")
    sys.exit(0 if resultado else 1)
