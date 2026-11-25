# =============================================================================
# Airflow (Astro Runtime 3.4 = Airflow 3.x, Python 3.14) + ambiente Spark
#
# O Astro Runtime usa Python 3.14 / Debian 13, que não suportam o PySpark 3.5
# nem têm Java 17 no apt. Por isso as tasks de Spark rodam num Python 3.11
# separado (/opt/spark-venv), chamado com @task.external_python, usando as
# mesmas versões do trabalho 2: pyspark 3.5.3 + delta-spark 3.2.1 + hadoop-aws 3.3.4.
# =============================================================================
FROM astrocrpublic.azurecr.io/runtime:3.4-1

USER root

# Java 17 (exigido pelo Spark 3.5)
COPY --from=eclipse-temurin:17-jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:${PATH}"

# Python 3.11 + venv do Spark (gerenciado pelo uv)
COPY --from=ghcr.io/astral-sh/uv:0.9.5 /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/uv-python \
    SPARK_VENV=/opt/spark-venv \
    SPARK_JARS_IVY=/opt/ivy
COPY spark-requirements.txt /tmp/spark-requirements.txt
RUN uv python install 3.11 \
 && uv venv "$SPARK_VENV" --python 3.11 \
 && uv pip install --python "$SPARK_VENV/bin/python" -r /tmp/spark-requirements.txt

# Baixa os JARs do Delta e do S3A já no build (as tasks não dependem de internet)
RUN mkdir -p "$SPARK_JARS_IVY" \
 && "$SPARK_VENV/bin/python" -c "from pyspark.sql import SparkSession; \
SparkSession.builder.master('local[1]') \
.config('spark.jars.packages','io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4') \
.config('spark.jars.ivy','$SPARK_JARS_IVY').getOrCreate().stop()" \
 && chown -R astro:astro "$SPARK_JARS_IVY"

USER astro
