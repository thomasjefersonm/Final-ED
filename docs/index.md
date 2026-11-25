# Final-ED · Eleição Presidencial 2026

Trabalho final da disciplina de **Engenharia de Dados** (Engenharia de Software · UNISATC).

> Construir um pipeline de dados completo: coletar os dados de uma origem, copiar para um **Data Lake**, transformar, refinar e disponibilizar os dados em **modelo dimensional ou OBT** para consumo em um **dashboard**.

## Visão geral

```
PostgreSQL (origem)      MinIO (Data Lake / object storage S3)
eleicao_2026_arq   ──▶   landing ──▶ bronze ──▶ silver ──▶ gold ──▶ Dashboard
14 tabelas (TSE)         CSV         Delta      Delta      Delta
```

| Etapa | Situação |
|---|---|
| Banco de origem (PostgreSQL, 14 tabelas) | ✅ pronto |
| Data Lake em object storage (MinIO em Docker) + buckets medalhão | ✅ pronto |
| Extração origem → landing (CSV) | ⏳ próxima etapa |
| Spark + Delta Lake (bronze, silver, gold) | ⏳ |
| Orquestração (ingestão e transformações agendadas) | ⏳ |
| Modelagem dimensional na gold (SCD tipo 2 nas dimensões, fato incremental) | ⏳ |
| Dashboard (4 KPIs + 2 métricas, One Page View) | ⏳ |

## Premissas do professor (resumo)

- Origem relacional com **no mínimo 10 tabelas**, **10.000 linhas** nas tabelas principais e datas distribuídas nos **últimos 3 anos**.
- Ingestão e movimentação entre camadas feitas por **ferramenta de orquestração** (local/Docker ou cloud) — **não** usar Agendador do Windows nem cron.
- Data Lake em **object storage** com arquitetura medalhão: **Landing** (formato bruto: CSV para SQL), **Bronze**, **Silver** e **Gold** em **Delta Lake**.
- Motor de transformação: **Apache Spark** (PySpark).
- Gold em **fatos e dimensões** (ou OBT); dimensões com **SCD tipo 2**; fatos com **carga incremental**.
- Documentação no **MkDocs**, versionamento via **Pull Request** com a `main` protegida e tarefas registradas em **Issues**.

## Páginas

- [Ambiente (Docker + MinIO)](ambiente.md): como subir, acessar e testar o Data Lake local.
- [Arquitetura medalhão](arquitetura.md): buckets, convenções de caminhos e equivalência com o trabalho 3 (Databricks).
