# ADF — Pipeline notes (portfolio)

Queste note descrivono il flusso tipico usato in azienda per ingestare una nuova tabella e popolare il lakehouse (bronze/silver/gold).

## Obiettivo
- Collegarsi a una sorgente (linked service)
- Copiare i dati in ADLS (landing/bronze raw)
- Registrare metadata su Databricks (Unity Catalog)
- Popolare le tabelle Delta (bronze/silver/gold) tramite notebook Databricks

## Componenti ADF (alto livello)
- **Linked Service (Source)**: connessione alla sorgente (DB / API / ecc.)
- **Linked Service (Sink)**: connessione a ADLS Gen2 / Blob
- **Dataset**: definizione di source e sink (path, formato, ecc.)
- **Pipeline**: orchestrazione delle activity

## Parametri tipici di pipeline (esempio)
- `p_source_table` : nome tabella sorgente
- `p_sink_path` : path di destinazione su ADLS (landing/raw)
- `p_dataset_name` : nome dataset (usato per naming cartelle)
- `p_enable_databricks` : flag per abilitare/disabilitare attività Databricks
- `p_env` : DEV/TEST/PROD (opzionale)

## Flusso operativo (nuova tabella)

### Step 1 — Preparazione
1) Verifica sorgente: tabella esiste? colonne? volume dati?
2) Crea o riusa pipeline esistente della stessa sorgente
3) Aggiorna i parametri (nome tabella, path sink, dataset, ecc.)

### Step 2 — Prima run “solo copy” (Databricks disabilitato)
Scopo: verificare che l’ingestion raw funzioni senza errori.
- Disabilita (temporaneamente) i blocchi/activity Databricks in pipeline
- Esegui pipeline:
  - output atteso: i dati vengono scritti su ADLS (landing / bronze raw)

### Step 3 — Create metadata su Databricks (Unity Catalog)
Scopo: registrare le tabelle nel metastore, pronte per query/BI.
- Esegui notebook Databricks “register tables”:
  - `CREATE TABLE ... USING DELTA LOCATION 'abfss://...'`
- Nota: la location deve puntare alla cartella Delta del layer (bronze/silver/gold).

### Step 4 — Run completa (Databricks abilitato)
Scopo: popolare Delta tables (bronze/silver/gold).
- Riabilita i blocchi Databricks in pipeline
- Esegui pipeline completa:
  - i notebook/jobs Databricks leggono raw e scrivono Delta nei layer
  - a fine run: `SELECT * FROM gold...` deve mostrare i record

## Output attesi
- **ADLS**: cartelle/layer con file raw e Delta
- **Databricks/UC**: tabelle registrate bronze/silver/gold
- **Gold**: tabelle “BI-ready” (es. `*_actual`, `*_scd2`)

## Note
- Questo repo non include export JSON reali di pipeline/linked service.
- I nomi e i path sono esempi (placeholder) per scopi portfolio.