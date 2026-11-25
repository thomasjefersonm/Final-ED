"""
## Pipeline Eleição Presidencial 2026 (arquitetura medalhão)

Agendado pelo **Airflow** (não usa cron nem Agendador do Windows): todo dia às 02h.

    listar_tabelas ──▶ extracao_landing (1 task por tabela, em paralelo)
                       Postgres → CSV em s3://landing/<tabela>/

Próximas etapas (entram nesta mesma DAG): landing → bronze → silver → gold.

O código de cada etapa fica em `include/pipeline/`; a DAG só define a ordem
e o agendamento.
"""

import os

import pendulum
from airflow.sdk import dag, task

SPARK_PYTHON = os.getenv("SPARK_VENV", "/opt/spark-venv") + "/bin/python"


@dag(
    dag_id="pipeline_eleicao_2026",
    schedule="0 2 * * *",  # todo dia às 02:00 (horário de Brasília)
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=1)},
    tags=["eleicao_2026", "medalhao", "landing"],
    doc_md=__doc__,
)
def pipeline_eleicao_2026():
    @task.external_python(python=SPARK_PYTHON)
    def listar_tabelas() -> list[dict]:
        import sys
        from datetime import datetime
        from zoneinfo import ZoneInfo

        sys.path.insert(0, "/usr/local/airflow/include")
        from pipeline.extracao import listar_tabelas as listar

        carimbo = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y%m%d_%H%M%S")
        tabelas = listar()
        print(f"{len(tabelas)} tabelas, carimbo {carimbo}: {tabelas}")
        return [{"tabela": t, "carimbo": carimbo} for t in tabelas]

    @task.external_python(
        python=SPARK_PYTHON,
        map_index_template="{{ task.op_kwargs['item']['tabela'] }}",
        max_active_tis_per_dag=4,  # no máximo 4 tabelas ao mesmo tempo no Postgres
    )
    def extracao_landing(item: dict) -> dict:
        import sys

        sys.path.insert(0, "/usr/local/airflow/include")
        from pipeline.extracao import extrair_tabela

        return extrair_tabela(item["tabela"], item["carimbo"])

    extracao_landing.expand(item=listar_tabelas())


pipeline_eleicao_2026()
