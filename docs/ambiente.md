# Ambiente (Docker + MinIO)

> **Em poucas palavras:** o MinIO é um "S3 dentro do nosso computador". Ele roda em um container Docker e guarda os arquivos do Data Lake em quatro buckets, um para cada camada da arquitetura medalhão.

## Pré-requisitos

| Item | Versão |
|---|---|
| WSL (Ubuntu) | 24.04 |
| Docker e Docker Compose | v2+ (Docker Engine instalado no WSL) |
| Python | 3.11 |
| UV | gerenciador de pacotes ([docs.astral.sh/uv](https://docs.astral.sh/uv/)) |
| Git | – |

## Containers

O arquivo `docker-compose.yml` (raiz do projeto) define o projeto Compose `final-ed`:

| Serviço | Container | Imagem | Portas (host → container) | Função |
|---|---|---|---|---|
| `minio` | `final-ed-minio` | `pgsty/minio:RELEASE.2026-08-04T00-00-00Z` | `9020 → 9000` (API S3), `9021 → 9001` (console) | Object storage do Data Lake |
| `minio-init` | `final-ed-minio-init` | mesma imagem | – | Cria os buckets e encerra (`Exited (0)`) |

Os dados ficam no volume Docker nomeado **`final-ed_minio-data`**, então sobrevivem a `docker compose down` e a reinícios da máquina.

!!! warning "Por que não `minio/minio`?"
    Nos trabalhos 1 e 2 (e no repositório modelo do professor) usamos `minio/minio:RELEASE.2025-02-03T21-03-04Z`.
    A MinIO Inc. **deixou de publicar imagens Docker** e esse repositório não existe mais no Docker Hub nem no `quay.io`
    (`pull access denied`). Usamos o fork mantido pela comunidade **`pgsty/minio`**: mesmo código-fonte (AGPLv3),
    mesmas variáveis (`MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD`), mesmas portas e o mesmo cliente `mc`, com o console web completo.
    A versão está **fixada** para que todos do grupo rodem exatamente o mesmo ambiente.

## Subindo o ambiente

```bash
cp .env.example .env        # preencher PG_HOST, PG_USER, PG_PASSWORD...
uv sync                     # cria o ambiente Python
uv run task up              # docker compose up -d
docker compose ps -a        # minio = healthy, minio-init = Exited (0)
docker logs final-ed-minio-init
```

Saída esperada do `minio-init`:

```
Bucket created successfully `lake/landing`.
Bucket created successfully `lake/bronze`.
Bucket created successfully `lake/silver`.
Bucket created successfully `lake/gold`.
```

A criação é idempotente (`mc mb --ignore-existing`): rodar `docker compose up -d` de novo não apaga nem recria nada.

## Acessos

| O quê | Endereço | Usuário / senha |
|---|---|---|
| Console web do MinIO | <http://localhost:9021> | `minioadmin` / `minioadmin` |
| API S3 (Spark, boto3, mc) | `http://localhost:9020` | `minioadmin` / `minioadmin` |
| API S3 de dentro da rede Docker | `http://minio:9000` | idem |

As credenciais servem **apenas** para este ambiente local de estudo e podem ser alteradas no `.env`.

## Testando (smoke test)

```bash
uv run task testar
```

O script `scripts/testar_ambiente.py`:

1. conecta no MinIO, confere se os buckets `landing`, `bronze`, `silver` e `gold` existem e grava, lê e apaga um arquivo de teste em cada um;
2. conecta no PostgreSQL de origem e conta as linhas de todas as tabelas do schema `public`.

```
== MinIO ==
  [OK] landing: escrita/leitura/remoção
  [OK] bronze: escrita/leitura/remoção
  [OK] silver: escrita/leitura/remoção
  [OK] gold: escrita/leitura/remoção
== PostgreSQL de origem ==
  apuracao_secao                 998,496 linhas
  ...
  votacao_secao                3,833,640 linhas
  [OK] 14 tabelas no schema 'public'

Ambiente OK
```

## Atalhos (taskipy)

| Comando | O que faz |
|---|---|
| `uv run task up` | `docker compose up -d` (sobe o MinIO e cria os buckets) |
| `uv run task down` | `docker compose down` (para, **mantém** os dados) |
| `uv run task destruir` | `docker compose down -v` (para e **apaga** os dados) |
| `uv run task logs` | logs do MinIO |
| `uv run task testar` | smoke test do ambiente |
| `uv run task docs` | MkDocs em <http://127.0.0.1:8000> |
| `uv run task build` | gera o site estático (`mkdocs build --strict`) |
| `uv run task deploy` | publica no GitHub Pages (`mkdocs gh-deploy`) |
