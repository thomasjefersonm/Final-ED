# Final-ED · Eleição Presidencial 2026

[![Docs](https://img.shields.io/badge/docs-mkdocs-blue)](https://thomasjefersonm.github.io/Final-ED/)

Trabalho final – **Pipeline de dados com Data Lake, arquitetura medalhão, Apache Spark e Delta Lake**
Engenharia de Dados · Engenharia de Software · UNISATC · Prof. Jorge Luiz da Silva

> 📚 Documentação completa (MkDocs): https://thomasjefersonm.github.io/Final-ED/

## Integrantes
- Thomas Jeferson da Silva Maggi
- Bruno Ghisi da Silva
- _(completar com os demais integrantes do grupo)_

## ⚠️ Aviso
Projeto acadêmico executado localmente com containers Docker. As credenciais do MinIO abaixo servem **apenas** para este ambiente de estudo. As credenciais do banco de origem ficam no `.env`, que **não** é versionado.

## Desenho de arquitetura

```
┌──────────────────────┐    ┌──────────────────────── MinIO (Docker) ────────────────────────┐
│ PostgreSQL (origem)  │    │                                                                 │
│ eleicao_2026_arq     │───▶│  landing  ──▶  bronze  ──▶  silver  ──▶  gold   ──────────────▶│──▶ Dashboard
│ 14 tabelas (TSE)     │    │  (CSV)        (Delta)      (Delta)      (Delta: fatos/dims)     │    (4 KPIs + 2 métricas)
└──────────────────────┘    └─────────────────────────────────────────────────────────────────┘
                                        Apache Spark (PySpark) + orquestrador
```

| Etapa | Situação |
|---|---|
| Banco de origem (PostgreSQL, 14 tabelas) | ✅ |
| Data Lake no MinIO + buckets `landing`, `bronze`, `silver`, `gold` | ✅ |
| Extração origem → landing (CSV) | ⏳ |
| Spark + Delta Lake (bronze, silver, gold) | ⏳ |
| Orquestração | ⏳ |
| Gold dimensional (SCD2 + fato incremental) | ⏳ |
| Dashboard | ⏳ |

## Ferramentas utilizadas

| Ferramenta | Versão | Para que serve |
|---|---|---|
| PostgreSQL | 18 | Banco relacional de origem (OLTP normalizado, dados do TSE) |
| MinIO (`pgsty/minio`) | RELEASE.2026-08-04T00-00-00Z | Object storage S3 do Data Lake (buckets da arquitetura medalhão) |
| Docker e Docker Compose | v2+ | Executam o MinIO em containers |
| Python | 3.11 | Scripts do pipeline |
| UV | – | Gerenciador de pacotes e ambiente virtual |
| boto3, pg8000, python-dotenv | ver `pyproject.toml` | Acesso ao MinIO, ao PostgreSQL e leitura do `.env` |
| Taskipy | – | Atalhos de comandos (`up`, `down`, `testar`, `docs`...) |
| MkDocs + Material | 1.6 / 9.x | Documentação publicada no GitHub Pages |
| Git e GitHub | – | Versionamento via Pull Request e Issues |

> **Sobre a imagem do MinIO:** a imagem `minio/minio` usada nos trabalhos 1 e 2 foi removida do Docker Hub e do quay.io pela MinIO Inc. Usamos o fork da comunidade `pgsty/minio` (mesmo binário AGPLv3, mesmas variáveis e portas), com versão fixada. Detalhes na [documentação do ambiente](docs/ambiente.md).

## Pré-requisitos

| Item | Versão |
|---|---|
| WSL (Ubuntu) | 24.04 |
| Docker e Docker Compose | v2+ |
| Python | 3.11 |
| UV | https://docs.astral.sh/uv/ |
| Git | – |

## Instalação

1. Clonar o repositório:
```bash
git clone https://github.com/thomasjefersonm/Final-ED.git
cd Final-ED
```

2. Criar o arquivo de variáveis de ambiente e preencher a conexão do PostgreSQL de origem:
```bash
cp .env.example .env
```

3. Criar o ambiente Python:
```bash
uv sync
```

4. Subir o MinIO (cria os buckets automaticamente):
```bash
uv run task up
docker compose ps -a
```

| Container | Imagem | Portas | Situação esperada |
|---|---|---|---|
| `final-ed-minio` | `pgsty/minio:RELEASE.2026-08-04T00-00-00Z` | `9020` (API), `9021` (console) | `Up (healthy)` |
| `final-ed-minio-init` | idem | – | `Exited (0)` após criar os buckets |

| Serviço | Endereço | Usuário | Senha |
|---|---|---|---|
| Console do MinIO | http://localhost:9021 | `minioadmin` | `minioadmin` |
| API S3 | http://localhost:9020 | `minioadmin` | `minioadmin` |

5. Testar o ambiente (MinIO + PostgreSQL):
```bash
uv run task testar
```

## Como usar

| Comando | O que faz |
|---|---|
| `uv run task up` | Sobe o MinIO e cria os buckets `landing`, `bronze`, `silver`, `gold` |
| `uv run task down` | Para os containers (mantém os dados) |
| `uv run task destruir` | Para os containers e **apaga** o volume de dados |
| `uv run task logs` | Logs do MinIO |
| `uv run task testar` | Smoke test do MinIO e do banco de origem |

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
├── docker-compose.yml        # MinIO + criação dos buckets da arquitetura medalhão
├── .env.example              # modelo das variáveis de ambiente (o .env não é versionado)
├── .python-version           # Python 3.11
├── .gitignore
├── .gitattributes
├── pyproject.toml            # dependências (UV) e atalhos (taskipy)
├── mkdocs.yml
├── scripts/
│   └── testar_ambiente.py    # smoke test do MinIO e do PostgreSQL
└── docs/                     # páginas do MkDocs
```

## Colaboração

1. Abra uma **issue** para a tarefa.
2. Crie uma **branch** a partir da `main`:
   ```bash
   git checkout -b feature/nome-da-feature
   ```
3. Faça os commits seguindo o [Conventional Commits](https://www.conventionalcommits.org/pt-br/v1.0.0/).
4. Abra um **Pull Request** para a `main` (a `main` é protegida e exige **2 aprovações**).

## Referências
- https://github.com/jlsilva01/projeto-ed-satc (repositório modelo do professor)
- https://github.com/jlsilva01/spark-delta-minio-sqlserver (Spark + MinIO + Delta, modelo do professor)
- https://github.com/thomasjefersonm/pesquisadados_parte2 (trabalho 2: Spark, MinIO e Delta Lake)
- https://github.com/thomasjefersonm/pesquisadados_parte3 (trabalho 3: Databricks e arquitetura medalhão)
- https://hub.docker.com/r/pgsty/minio
- https://docs.delta.io/
- https://www.mkdocs.org
- https://squidfunk.github.io/mkdocs-material/
