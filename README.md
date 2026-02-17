# Azure Lakehouse (SQL-first) — ADF + Databricks + Power BI

Repo portfolio: flusso **bronze / silver / gold** su Azure con ADF + Databricks + Power BI.

## Stack
- Azure Data Factory (ADF): orchestrazione ingestion
- Azure Storage / ADLS Gen2: layer bronze/silver/gold (file)
- Databricks (Unity Catalog): tabelle Delta + metadata (external tables)
- Power BI: consumo dati da layer gold

## Flusso (alto livello)
1) Ingestion in bronze (file raw)
2) Create metadata (Unity Catalog) e registrazione tabelle su ADLS
3) Pulizia e tipizzazione in silver
4) Dataset BI-ready in gold (**actual / scd2**, viste/aggregazioni)
5) Power BI collegato a Databricks per dashboard

## Struttura repo
- `adf/pipelines/`
  - `pipeline_notes.md` : note su linked service, parametri e flusso operativo (run senza/con Databricks)
- `databricks/notebooks/`
  - `00_dq_dictionary_setup.py` : setup dizionario DQ (regole metadata-driven)
  - `01_first_ingestion_update.py` : first ingestion + update (append + partizione)
  - `02_register_tables_bronze_silver_gold.py` : create metadata (register external Delta tables via LOCATION)
  - `03_gold_scd2_template.py` : template portfolio per gestione **SCD2** (gold actual → gold scd2)
- `powerbi/`
  - `model_notes.md` : connessione Power BI ↔ Databricks + modello e misure DAX esempio
- `architecture/`
  - `architecture.md` : panoramica architettura e flusso dati
  - `scd2.md` : note operative sul pattern **SCD2** (gold)

## Git hygiene
- `.gitignore` evita di versionare file locali, pesanti o sensibili (es. `.env`, chiavi, `.pbix`, cartelle editor).

## Note
Questo progetto non contiene credenziali o dati sensibili.