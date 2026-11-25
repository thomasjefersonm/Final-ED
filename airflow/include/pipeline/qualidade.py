"""Validações de qualidade de dados na camada Silver (Delta Lake).

Verifica, para todas as tabelas da Silver:
1. Existência da tabela Delta e ausência de tabelas vazias.
2. Tipos incorretos (conformidade de schema com o contrato).
3. Valores nulos indevidos em colunas obrigatórias (NOT NULL).
4. Duplicidades em chaves primárias (PK) e chaves candidatas (UNIQUE).
5. Inconsistências de regras de negócio (CHECKs do schema OLTP) e integridade
   referencial entre tabelas (FKs via left_anti join).
"""

import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipeline.silver import ESPECIFICACOES_TABELAS

# relacionamentos de integridade referencial: (tabela_filha, cols_fk, tabela_pai, cols_pk)
CHAVES_ESTRANGEIRAS: list[tuple[str, list[str], str, list[str]]] = [
    ("turno", ["id_eleicao"], "eleicao", ["id_eleicao"]),
    ("municipio", ["sg_uf"], "uf", ["sg_uf"]),
    ("zona", ["sg_uf"], "uf", ["sg_uf"]),
    ("local_votacao", ["cd_municipio"], "municipio", ["cd_municipio"]),
    ("local_votacao", ["id_zona"], "zona", ["id_zona"]),
    ("secao", ["id_zona"], "zona", ["id_zona"]),
    ("secao", ["cd_municipio"], "municipio", ["cd_municipio"]),
    ("candidato", ["cd_cargo"], "cargo", ["cd_cargo"]),
    ("candidato", ["nr_partido"], "partido", ["nr_partido"]),
    ("apuracao_secao", ["id_turno"], "turno", ["id_turno"]),
    ("apuracao_secao", ["id_secao"], "secao", ["id_secao"]),
    ("apuracao_secao", ["id_local_votacao"], "local_votacao", ["id_local_votacao"]),
    ("votacao_secao", ["id_turno", "id_secao"], "apuracao_secao", ["id_turno", "id_secao"]),
    ("votacao_secao", ["sq_candidato"], "candidato", ["sq_candidato"]),
    ("totalizacao", ["id_turno"], "turno", ["id_turno"]),
    ("totalizacao", ["cd_municipio"], "municipio", ["cd_municipio"]),
    ("totalizacao", ["id_zona"], "zona", ["id_zona"]),
    ("totalizacao_candidato", ["id_totalizacao"], "totalizacao", ["id_totalizacao"]),
    ("totalizacao_candidato", ["sq_candidato"], "candidato", ["sq_candidato"]),
]

# regras de consistência de domínio (espelham os CHECKs de 01_schema.sql)
REGRAS_DOMINIO: dict[str, list[tuple[str, str]]] = {
    "turno": [
        ("nr_turno_valido", "nr_turno NOT IN (1, 2)"),
    ],
    "apuracao_secao": [
        (
            "quantidades_nao_negativas",
            "qt_aptos < 0 OR qt_comparecimento < 0 OR qt_abstencoes < 0 "
            "OR qt_votos_nominais < 0 OR qt_votos_brancos < 0 OR qt_votos_nulos < 0 "
            "OR qt_votos_legenda < 0 OR qt_votos_anulados_apu_sep < 0",
        ),
        (
            "ck_apuracao_aptos",
            "st_secao_instalada = true AND (qt_comparecimento + qt_abstencoes) <> qt_aptos",
        ),
        (
            "ck_apuracao_votos",
            "(qt_votos_nominais + qt_votos_brancos + qt_votos_nulos "
            "+ qt_votos_legenda + qt_votos_anulados_apu_sep) <> qt_comparecimento",
        ),
    ],
    "votacao_secao": [
        ("qt_votos_nao_negativo", "qt_votos < 0"),
    ],
    "totalizacao": [
        (
            "totais_nao_negativos",
            "qt_secoes_tot < 0 OR qt_secoes_tot_acumulado < 0 "
            "OR qt_aptos_tot < 0 OR qt_aptos_tot_acumulado < 0 "
            "OR qt_votos_brancos < 0 OR qt_votos_brancos_acumulado < 0 "
            "OR qt_votos_nulos < 0 OR qt_votos_nulos_acumulado < 0",
        ),
    ],
    "totalizacao_candidato": [
        ("votos_nao_negativos", "qt_votos < 0 OR qt_votos_acumulado < 0"),
    ],
}


def _bucket(camada: str) -> str:
    return os.getenv(f"MINIO_{camada.upper()}_BUCKET", camada)


def _checar_tipos(df: DataFrame, spec: dict) -> list[str]:
    erros = []
    tipos_atuais = {campo.name: campo.dataType.simpleString() for campo in df.schema.fields}
    for col, tipo_esperado in spec["colunas"].items():
        tipo_atual = tipos_atuais.get(col)
        if tipo_atual is None:
            erros.append(f"coluna ausente: {col}")
        elif tipo_atual != tipo_esperado:
            erros.append(f"tipo incorreto em '{col}': esperado={tipo_esperado}, atual={tipo_atual}")
    return erros


def _checar_nulos(df: DataFrame, colunas_not_null: list[str]) -> list[str]:
    if not colunas_not_null:
        return []
    expressoes = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in colunas_not_null
    ]
    contagens = df.agg(*expressoes).collect()[0].asDict()
    return [
        f"nulos indevidos em '{col}': {qtd} linha(s)"
        for col, qtd in contagens.items()
        if qtd and qtd > 0
    ]


def _checar_duplicidades(df: DataFrame, grupos_chaves: list[tuple[str, list[str]]]) -> list[str]:
    erros = []
    for rotulo, cols in grupos_chaves:
        duplicadas = (
            df.groupBy(*cols)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        if duplicadas > 0:
            erros.append(f"duplicidade em {rotulo} ({', '.join(cols)}): {duplicadas} chave(s)")
    return erros


def _checar_dominio(df: DataFrame, regras: list[tuple[str, str]]) -> list[str]:
    erros = []
    for nome_regra, condicao_invalida in regras:
        violacoes = df.filter(F.expr(condicao_invalida)).count()
        if violacoes > 0:
            erros.append(f"regra '{nome_regra}' violada: {violacoes} linha(s)")
    return erros


def _checar_fks(dfs: dict[str, DataFrame]) -> list[str]:
    erros = []
    for filha, cols_fk, pai, cols_pk in CHAVES_ESTRANGEIRAS:
        if filha not in dfs or pai not in dfs:
            continue
        condicao = [dfs[filha][fk] == dfs[pai][pk] for fk, pk in zip(cols_fk, cols_pk)]
        orfaos = (
            dfs[filha]
            .select(*cols_fk)
            .dropna(subset=cols_fk)
            .join(dfs[pai].select(*cols_pk), on=condicao, how="left_anti")
            .count()
        )
        if orfaos > 0:
            erros.append(
                f"FK violada: {filha}({', '.join(cols_fk)}) -> "
                f"{pai}({', '.join(cols_pk)}): {orfaos} registro(s) órfão(s)"
            )
    return erros


def validar_silver() -> dict:
    from pipeline.spark import criar_spark

    spark = criar_spark("validar_qualidade_silver")
    falhas: dict[str, list[str]] = {}
    dfs_carregados: dict[str, DataFrame] = {}
    total_linhas: dict[str, int] = {}

    try:
        for tabela, spec in ESPECIFICACOES_TABELAS.items():
            caminho = f"s3a://{_bucket('silver')}/{tabela}"
            if not DeltaTable.isDeltaTable(spark, caminho):
                falhas[tabela] = ["tabela Delta não encontrada na camada Silver"]
                continue

            df = spark.read.format("delta").load(caminho)
            dfs_carregados[tabela] = df
            erros_tabela: list[str] = []

            qtd = df.count()
            total_linhas[tabela] = qtd
            if qtd == 0:
                erros_tabela.append("tabela vazia (0 linhas)")

            erros_tabela.extend(_checar_tipos(df, spec))
            erros_tabela.extend(_checar_nulos(df, spec["not_null"]))

            chaves = [("PK", spec["pk"])] + [
                ("UNIQUE", uq) for uq in spec.get("unicas", [])
            ]
            erros_tabela.extend(_checar_duplicidades(df, chaves))
            erros_tabela.extend(_checar_dominio(df, REGRAS_DOMINIO.get(tabela, [])))

            if erros_tabela:
                falhas[tabela] = erros_tabela
            else:
                print(f"[OK] {tabela}: {qtd} linhas validadas sem inconsistências.")

        erros_fk = _checar_fks(dfs_carregados)
        if erros_fk:
            falhas["integridade_referencial"] = erros_fk
    finally:
        spark.stop()

    relatorio = {
        "tabelas_validadas": len(dfs_carregados),
        "total_linhas": total_linhas,
        "falhas": falhas,
        "status": "falhou" if falhas else "aprovado",
    }
    print("Relatório de qualidade Silver:", relatorio)

    if falhas:
        detalhes = "; ".join(f"{tab}: {errs}" for tab, errs in falhas.items())
        raise ValueError(f"Validações de qualidade na Silver falharam -> {detalhes}")

    return relatorio