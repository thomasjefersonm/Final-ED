# Arquitetura medalhão

> **Em poucas palavras:** os dados saem do PostgreSQL e passam por quatro "gavetas" (buckets) no MinIO. A cada gaveta eles ficam mais limpos e organizados, até chegarem prontos para o dashboard.

## Camadas (buckets)

| Bucket | Camada | Formato | Conteúdo |
|---|---|---|---|
| `landing` | Landing | **CSV** (bruto original) | Cópia fiel das tabelas da origem, sem transformação |
| `bronze` | Bronze | **Delta Lake** | Dados da landing + colunas de auditoria (arquivo de origem, data/hora da carga) |
| `silver` | Silver | **Delta Lake** | Data quality: tipagem, padronização de nomes, deduplicação, regras de negócio |
| `gold` | Gold | **Delta Lake** | Modelo dimensional (fatos e dimensões / OBT) consumido pelo dashboard |

Os nomes dos buckets ficam no `.env` (`MINIO_BUCKETS`, `MINIO_*_BUCKET`), para que os scripts não tenham caminhos fixos no código.

## Convenção de caminhos (proposta)

```
s3a://landing/<tabela>/<tabela>_<yyyymmdd_hhmmss>.csv
s3a://bronze/<tabela>/          (tabela Delta)
s3a://silver/<tabela>/          (tabela Delta)
s3a://gold/dim_<nome>/          (dimensão, SCD tipo 2)
s3a://gold/fato_<nome>/         (fato, carga incremental)
```

Guardar o carimbo de data/hora no nome do arquivo da landing permite reprocessar uma carga específica e demonstrar a **carga incremental** na apresentação.

## Equivalência com o trabalho 3 (Databricks)

No trabalho 3 o Databricks entregava tudo pronto (Unity Catalog, volumes, Jobs). Aqui montamos o equivalente "na mão":

| Trabalho 3 (Databricks) | Trabalho final (local) |
|---|---|
| Notebook `001_preparando_ambiente` (CREATE SCHEMA/VOLUME) | Serviço `minio-init` do `docker-compose.yml` |
| Volume `workspace.landing.dados` | Bucket `landing` |
| Schema `workspace.bronze` | Bucket `bronze` |
| Schema `workspace.silver` | Bucket `silver` |
| Schema `workspace.gold` | Bucket `gold` |
| Parâmetros do Job (`pg_host`, `pg_password`...) | Variáveis do `.env` (fora do Git) |
| Notebook `006_destruindo_ambiente` | `uv run task destruir` (`docker compose down -v`) |
| Jobs & Pipelines | Ferramenta de orquestração (próxima etapa) |

## Banco de origem

PostgreSQL `eleicao_2026_arq`, schema `public`, modelo OLTP normalizado com os dados do TSE (Presidente 2026, 1º e 2º turno):

| Tabela | Linhas | Papel |
|---|---:|---|
| `votacao_secao` | 3.833.640 | votos nominais por seção, turno e candidato |
| `totalizacao_candidato` | 1.077.999 | evolução da apuração por candidato |
| `apuracao_secao` | 998.496 | comparecimento, abstenções, brancos e nulos por seção |
| `secao` | 499.248 | seções eleitorais |
| `totalizacao` | 158.669 | evolução da apuração por município/zona |
| `local_votacao` | 94.343 | locais de votação |
| `municipio` | 5.757 | municípios (código TSE) |
| `zona` | 2.641 | zonas eleitorais |
| `partido` | 30 | partidos |
| `uf` | 28 | 27 UFs + exterior |
| `candidato` | 13 | candidatos (situação atualizada entre turnos → SCD2) |
| `turno` | 2 | 1º e 2º turno |
| `cargo` | 1 | Presidente |
| `eleicao` | 1 | Eleição geral 2026 |
