"""Extração: PostgreSQL de origem -> bucket landing do MinIO (CSV bruto).

Cada tabela vira um arquivo CSV em:
    s3://landing/<tabela>/<tabela>_<carimbo>.csv

Tabelas com coluna de controle (TABELAS_INCREMENTAIS) são extraídas de forma
incremental: só entram as linhas com coluna > último valor já extraído. Esse
valor fica num checkpoint no próprio Data Lake:
    s3://landing/_checkpoint/<tabela>.json
As demais tabelas (pequenas, de cadastro) são copiadas inteiras a cada carga.

A leitura é feita em blocos com cursor no servidor (DECLARE/FETCH), para não
carregar milhões de linhas na memória.
"""

import csv
import json
import os
import tempfile
from datetime import datetime

import boto3
import pg8000.dbapi
from botocore.exceptions import ClientError

# tabela -> coluna usada como controle da carga incremental
TABELAS_INCREMENTAIS = {
    "apuracao_secao": "dt_carga",
    "votacao_secao": "dt_carga",
    "totalizacao": "dt_carga",
}
TAMANHO_BLOCO = 100_000


def _conectar_postgres():
    return pg8000.dbapi.connect(
        host=os.environ["PG_HOST"],
        port=int(os.environ["PG_PORT"]),
        database=os.environ["PG_DATABASE"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
        timeout=60,
    )


def _cliente_s3():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1",
    )


def _bucket_landing() -> str:
    return os.getenv("MINIO_LANDING_BUCKET", "landing")


def listar_tabelas() -> list[str]:
    """Lista as tabelas do schema de origem (a lista vem do próprio banco)."""
    schema = os.getenv("PG_SCHEMA", "public")
    con = _conectar_postgres()
    try:
        cur = con.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s AND table_type = 'BASE TABLE' ORDER BY 1",
            (schema,),
        )
        return [linha[0] for linha in cur.fetchall()]
    finally:
        con.close()


def ler_checkpoint(s3, tabela: str) -> str | None:
    try:
        obj = s3.get_object(Bucket=_bucket_landing(), Key=f"_checkpoint/{tabela}.json")
        return json.loads(obj["Body"].read())["ultimo_valor"]
    except ClientError as erro:
        if erro.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise


def gravar_checkpoint(s3, tabela: str, coluna: str, valor: str, carimbo: str) -> None:
    corpo = {"tabela": tabela, "coluna": coluna, "ultimo_valor": valor, "carga": carimbo}
    s3.put_object(
        Bucket=_bucket_landing(),
        Key=f"_checkpoint/{tabela}.json",
        Body=json.dumps(corpo, ensure_ascii=False, indent=2).encode(),
        ContentType="application/json",
    )


def extrair_tabela(tabela: str, carimbo: str) -> dict:
    """Extrai uma tabela para a landing. Retorna um resumo da carga."""
    schema = os.getenv("PG_SCHEMA", "public")
    s3 = _cliente_s3()
    coluna = TABELAS_INCREMENTAIS.get(tabela)
    checkpoint = ler_checkpoint(s3, tabela) if coluna else None

    sql = f'SELECT * FROM "{schema}"."{tabela}"'
    params: tuple = ()
    if coluna and checkpoint:
        sql += f' WHERE "{coluna}" > %s'
        params = (checkpoint,)
    tipo = "incremental" if coluna and checkpoint else "completa"

    con = _conectar_postgres()
    linhas = 0
    maior_valor = None
    with tempfile.NamedTemporaryFile("w", suffix=".csv", newline="", encoding="utf-8",
                                     delete=False) as tmp:
        caminho_tmp = tmp.name
        try:
            cur = con.cursor()
            cur.execute(f"DECLARE cur_extracao NO SCROLL CURSOR FOR {sql}", params)
            escritor = csv.writer(tmp)
            idx_coluna = None
            cabecalho_escrito = False
            while True:
                cur.execute(f"FETCH FORWARD {TAMANHO_BLOCO} FROM cur_extracao")
                bloco = cur.fetchall()
                if not cabecalho_escrito:
                    colunas = [c[0] for c in cur.description]
                    escritor.writerow(colunas)
                    idx_coluna = colunas.index(coluna) if coluna else None
                    cabecalho_escrito = True
                if not bloco:
                    break
                escritor.writerows(bloco)
                linhas += len(bloco)
                if idx_coluna is not None:
                    maior_bloco = max(linha[idx_coluna] for linha in bloco)
                    if maior_valor is None or maior_bloco > maior_valor:
                        maior_valor = maior_bloco
            cur.execute("CLOSE cur_extracao")
            con.rollback()  # só leitura: encerra a transação do cursor
        finally:
            con.close()

    chave = None
    try:
        if linhas > 0:
            chave = f"{tabela}/{tabela}_{carimbo}.csv"
            s3.upload_file(caminho_tmp, _bucket_landing(), chave,
                           ExtraArgs={"ContentType": "text/csv"})
            if coluna and maior_valor is not None:
                valor = maior_valor.isoformat() if isinstance(maior_valor, datetime) else str(maior_valor)
                gravar_checkpoint(s3, tabela, coluna, valor, carimbo)
    finally:
        os.remove(caminho_tmp)

    resumo = {"tabela": tabela, "tipo": tipo, "linhas": linhas,
              "arquivo": f"s3://{_bucket_landing()}/{chave}" if chave else None,
              "checkpoint_anterior": checkpoint}
    print(resumo)
    return resumo
