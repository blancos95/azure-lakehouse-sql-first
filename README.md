# Azure Lakehouse (SQL-first) — ADF + Databricks + Power BI

Repo portfolio: flusso **bronze / silver / gold** su Azure con ADF + Databricks (SQL-first) + Power BI.

## Stack
- Azure Data Factory (ADF): orchestrazione ingestion
- Azure Storage / ADLS Gen2: layer bronze/silver/gold (file)
- Databricks: trasformazioni e tabelle curate (Delta)
- Power BI: consumo dati da layer gold

## Flusso (alto livello)
1) Ingestion in bronze (file raw)
2) Pulizia e tipizzazione in silver
3) Dataset BI-ready in gold (viste/aggregazioni)
4) Power BI collegato a Databricks per dashboard

## Struttura repo
- `adf/pipelines/` : pipeline / note di configurazione
- `databricks/notebooks/` : script SQL per bronze/silver/gold + esempio MERGE
- `powerbi/` : note su modello e misure
- `architecture/` : diagramma architettura (in arrivo)

## Git hygiene
- `.gitignore` evita di versionare file locali, pesanti o sensibili (es. `.env`, chiavi, `.pbix`, cartelle editor).

## Note
Questo progetto non contiene credenziali o dati sensibili.
