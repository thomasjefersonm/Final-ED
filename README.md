# Final-ED · Eleição Presidencial 2026

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-mkdocs-blue)](https://thomasjefersonm.github.io/Final-ED/)

Trabalho final – **Pipeline de dados com Data Lake, arquitetura medalhão, Apache Spark e Delta Lake**
Engenharia de Dados · Engenharia de Software · UNISATC · Prof. Jorge Luiz da Silva

> 📚 Documentação (MkDocs): https://thomasjefersonm.github.io/Final-ED/ _(em construção)_

## Integrantes
- Thomas Jeferson da Silva Maggi
- Bruno Ghisi da Silva
- _(completar com os demais integrantes do grupo)_

## ⚠️ Aviso
Projeto acadêmico executado localmente com containers Docker. As credenciais do MinIO abaixo servem **apenas** para este ambiente de estudo. As credenciais do banco de origem ficam no `.env`, que **não** é versionado.

## Desenho de arquitetura

```
                         ┌──────────────── Airflow (Astro CLI) ────────────────┐
                         │  DAG pipeline_eleicao_2026 · todo dia às 02h        │
                         └──────────────────────────┬──────────────────────────┘
                                                    │ orquestra
┌──────────────────────┐    ┌───────────────────────▼──── MinIO (Docker) ─────────────────────┐
│ PostgreSQL (origem)  │    │                                                                  │
│ eleicao_2026_arq     │───▶│  landing  ──▶  bronze  ──▶  silver  ──▶  gold   ─────────────────│──▶ Dashboard
│ 14 tabelas (TSE)     │    │  (CSV)        (Delta)      (Delta)      (Delta: fatos/dims)      │    (4 KPIs + 2 métricas)
└──────────────────────┘    └──────────────────────────────────────────────────────────────────┘
                                         Apache Spark (PySpark) + Delta Lake
```

| Etapa | Situação |
|---|---|
| Banco de origem normalizado (PostgreSQL, 14 tabelas) | ✅ |
| Data Lake no MinIO + buckets `landing`, `bronze`, `silver`, `gold` | ✅ |
| Orquestrador (Airflow via Astro CLI) | ✅ |
| Spark + Delta Lake + conector S3 rodando pelo Airflow | ✅ |
| Extração origem → landing (CSV), com carga incremental e agendamento | ✅ |
| Landing → Bronze (Delta) | ⏳ |
| Bronze → Silver (Delta + qualidade) | ⏳ |
| Gold dimensional (SCD2 + fato incremental) | ⏳ |
| Dashboard | ⏳ |

## Ferramentas utilizadas

| Ferramenta | Versão | Para que serve |
|---|---|---|
| PostgreSQL | 18 | Banco relacional de origem (OLTP normalizado, dados do TSE) |
| MinIO (`pgsty/minio`) | RELEASE.2026-08-04T00-00-00Z | Object storage S3 do Data Lake (buckets da arquitetura medalhão) |
| Apache Airflow (Astro CLI) | 3.x (Astro Runtime 3.4) | Orquestração e agendamento do pipeline |
| Apache Spark (PySpark) | 3.5.3 | Motor de transformação |
| Delta Lake (delta-spark) | 3.2.1 | Formato das camadas bronze, silver e gold |
| hadoop-aws (conector S3A) | 3.3.4 | Leitura e gravação do Spark no MinIO |
| Java (Temurin) | 17 | Necessário para o Spark (dentro da imagem do Airflow) |
| Docker e Docker Compose | v2+ | Executam o MinIO e o Airflow em containers |
| Python | 3.11 | Scripts do pipeline |
| UV | – | Gerenciador de pacotes e ambiente virtual |
| boto3, pg8000, python-dotenv | ver `pyproject.toml` | Acesso ao MinIO, ao PostgreSQL e leitura do `.env` |
| Taskipy | – | Atalhos de comandos (`up`, `down`, `testar`, `docs`...) |
| MkDocs + Material | 1.6 / 9.x | Documentação publicada no GitHub Pages |
| SonarQube Cloud | – | Análise de qualidade do código em cada Pull Request |
| Git e GitHub | – | Versionamento via Pull Request e Issues |

> **Sobre a imagem do MinIO:** a imagem `minio/minio` usada nos trabalhos 1 e 2 foi removida do Docker Hub e do quay.io pela MinIO Inc. Usamos o fork da comunidade `pgsty/minio` (mesmo binário AGPLv3, mesmas variáveis e portas), com versão fixada.

> **Sobre o Spark no Airflow:** o Astro Runtime usa Python 3.14, incompatível com o PySpark 3.5. Por isso as tasks de Spark rodam num Python 3.11 separado dentro da imagem (`@task.external_python`), mantendo as mesmas versões do trabalho 2.

## Pré-requisitos

| Item | Versão |
|---|---|
| WSL (Ubuntu) | 24.04 |
| Docker e Docker Compose | v2+ (Docker Engine no WSL ou Docker Desktop com integração WSL) |
| Astro CLI | `curl -sSL install.astronomer.io \| sudo bash -s` |
| Python | 3.11 |
| UV | https://docs.astral.sh/uv/ |
| Git | – |

Memória recomendada: 16 GB de RAM (com 8 GB funciona, ajustando o `.wslconfig`).

## Instalação

1. Clonar o repositório:
```bash
git clone https://github.com/thomasjefersonm/Final-ED.git
cd Final-ED
```

2. Criar o arquivo de variáveis de ambiente e preencher a conexão do PostgreSQL de origem (pedir os dados ao grupo):
```bash
cp .env.example .env
```

3. Criar o ambiente Python:
```bash
uv sync
```

4. Subir o MinIO (cria os buckets automaticamente):
```bash
docker compose up -d
docker compose ps -a
```

| Container | Imagem | Portas | Situação esperada |
|---|---|---|---|
| `final-ed-minio` | `pgsty/minio:RELEASE.2026-08-04T00-00-00Z` | `9020` (API), `9021` (console) | `Up (healthy)` |
| `final-ed-minio-init` | idem | – | `Exited (0)` após criar os buckets |

5. Subir o Airflow (**sempre depois do MinIO**, porque ele entra na rede do MinIO):
```bash
cd airflow
astro dev start
```
A primeira execução demora alguns minutos (monta a imagem com Java, Spark e Delta).

| Serviço | Endereço | Usuário | Senha |
|---|---|---|---|
| Airflow | http://localhost:8080 | – | – |
| Console do MinIO | http://localhost:9021 | `minioadmin` | `minioadmin` |
| API S3 | http://localhost:9020 | `minioadmin` | `minioadmin` |

6. Testar o ambiente: no Airflow, abrir a DAG **`teste_ambiente`** e clicar em **Trigger**. As 3 tasks devem ficar verdes (PostgreSQL → MinIO → Spark/Delta).

## Como usar

### DAGs

| DAG | Agendamento | O que faz |
|---|---|---|
| `teste_ambiente` | manual | Valida a conexão com o PostgreSQL, os buckets do MinIO e a gravação/leitura de uma tabela Delta pelo Spark |
| `pipeline_eleicao_2026` | todo dia às 02h | Extrai as 14 tabelas do PostgreSQL para `s3://landing/<tabela>/` em CSV (uma task por tabela) |

A extração é **incremental** nas tabelas grandes (`votacao_secao`, `apuracao_secao`, `totalizacao`): só entram as linhas com `dt_carga` maior que a última carga, registrada em `s3://landing/_checkpoint/<tabela>.json`. As tabelas de cadastro são copiadas inteiras a cada execução.

### Comandos

| Comando | O que faz |
|---|---|
| `docker compose up -d` | Sobe o MinIO e cria os buckets |
| `docker compose down` | Para o MinIO (mantém os dados) |
| `docker compose down -v` | Para o MinIO e **apaga** os dados |
| `cd airflow && astro dev start` | Sobe o Airflow |
| `cd airflow && astro dev stop` | Para o Airflow |
| `uv run task testar` | Smoke test do MinIO e do PostgreSQL pelo terminal |

## Documentação (MkDocs)
```bash
uv run task docs     # visualizar em http://127.0.0.1:8000
uv run task build    # gerar o site (mkdocs build --strict)
uv run task deploy   # publicar no GitHub Pages (mkdocs gh-deploy)
```

## Estrutura
```
Final-ED/
├── README.md
├── LICENSE
├── docker-compose.yml          # MinIO + criação dos buckets da arquitetura medalhão
├── .env.example                # modelo das variáveis de ambiente (o .env não é versionado)
├── pyproject.toml              # dependências (UV) e atalhos (taskipy)
├── mkdocs.yml
├── sonar-project.properties    # SonarQube Cloud
├── .github/workflows/          # análise do Sonar em cada PR
├── Banco_normalizado/          # DDL (01_schema.sql) e diagrama (origem.dbml) do banco de origem
├── airflow/                    # projeto do Airflow (Astro CLI)
│   ├── Dockerfile              # Airflow + Java 17 + Python 3.11 com PySpark/Delta
│   ├── spark-requirements.txt  # bibliotecas do ambiente Spark
│   ├── docker-compose.override.yml  # liga o Airflow à rede do MinIO e ao .env
│   ├── dags/
│   │   ├── teste_ambiente.py
│   │   └── pipeline_eleicao_2026.py
│   └── include/pipeline/
│       └── extracao.py         # extração PostgreSQL → landing (incremental)
├── scripts/
│   └── testar_ambiente.py      # smoke test do MinIO e do PostgreSQL
└── docs/                       # páginas do MkDocs
```

## Colaboração

1. Abra uma **issue** para a tarefa.
2. Crie uma **branch** a partir da `main`:
   ```bash
   git checkout -b feature/nome-da-feature
   ```
3. Faça os commits seguindo o [Conventional Commits](https://www.conventionalcommits.org/pt-br/v1.0.0/).
4. Abra um **Pull Request** para a `main` (a `main` é protegida e exige **2 aprovações**).

## Versão

O projeto segue o [Versionamento Semântico (SemVer)](https://semver.org/lang/pt-BR/), no formato `MAJOR.MINOR.PATCH`.

## Licença

Este projeto está sob a licença MIT – veja o arquivo [LICENSE](LICENSE).

## Referências
- https://github.com/jlsilva01/projeto-ed-satc (repositório modelo do professor)
- https://github.com/jlsilva01/spark-delta-minio-sqlserver (Spark + MinIO + Delta, modelo do professor)
- https://github.com/thomasjefersonm/pesquisadados_parte2 (trabalho 2: Spark, MinIO e Delta Lake)
- https://github.com/thomasjefersonm/pesquisadados_parte3 (trabalho 3: Databricks e arquitetura medalhão)
- https://www.astronomer.io/docs/astro/cli/overview
- https://airflow.apache.org/docs/
- https://hub.docker.com/r/pgsty/minio
- https://docs.delta.io/
- https://www.mkdocs.org
- https://squidfunk.github.io/mkdocs-material/
