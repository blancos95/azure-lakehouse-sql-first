# Architecture — Azure Lakehouse (Bronze / Silver / Gold)

Questo progetto mostra un flusso tipico lakehouse su Azure con **ADF + ADLS Gen2 + Databricks (Unity Catalog) + Power BI**.

## Componenti

- **Azure Data Factory (ADF)**
  - Orchestrazione ingestion
  - Copia dati da sorgente → ADLS (landing/bronze raw)
  - (Opzionale) esegue notebook Databricks per trasformazioni

- **ADLS Gen2 / Blob Storage**
  - Persistenza dei dati nei layer:
    - **Bronze (raw)**: file grezzi (es. CSV) come arrivano dalla sorgente
    - **Bronze (delta)**: tabella Delta “bronze” con colonne tecniche e partizioni
    - **Silver (delta)**: dati puliti e tipizzati
    - **Gold (delta)**: dataset BI-ready (actual / scd2, viste, aggregazioni)

- **Databricks + Unity Catalog**
  - Creazione/registrazione tabelle (metadata) su location ADLS
  - Trasformazioni (notebook/jobs) e scrittura in Delta
  - Query SQL su tabelle bronze/silver/gold

- **Power BI**
  - Connessione a Databricks (warehouse/SQL endpoint)
  - Consumo di tabelle/vista **Gold** per dashboard

## Flusso dati (alto livello)

1) **ADF → ADLS (Bronze raw)**
   - ADF copia i dati dalla sorgente e li deposita in una cartella di landing/bronze raw.
   - Esempio: file CSV in `bronze/<dataset>/`.

2) **Create metadata (Unity Catalog)**
   - In Databricks si registrano le tabelle bronze/silver/gold con:
     - `CREATE TABLE ... USING DELTA LOCATION 'abfss://...'`
   - Queste sono tabelle **esterne**: puntano a file Delta nello storage.

3) **Databricks jobs/notebook → Delta (Bronze/Silver/Gold)**
   - Notebook/Jobs trasformano i dati e scrivono tabelle Delta nei layer:
     - Bronze delta (con colonne tecniche e partizioni)
     - Silver (clean/typed)
     - Gold (BI-ready)

4) **Power BI → Gold**
   - Power BI si collega a Databricks e legge la Gold (es. `*_actual`, `*_scd2` o viste aggregate).

## Convenzioni (esempio)

- **Bronze**
  - raw: `abfss://.../bronze/<dataset>/` (file sorgente)
  - delta: `abfss://.../bronze/<dataset>/delta/<table>/` (Delta + _delta_log)

- **Silver**
  - `abfss://.../silver/<dataset>/delta/<table>/`

- **Gold**
  - actual: `abfss://.../gold/<dataset>/delta/actual/`
  - scd2: `abfss://.../gold/<dataset>/delta/scd2/`

## Note su Unity Catalog

- Le tabelle create con `USING DELTA LOCATION` sono **external tables**:
  - il metastore salva “metadata + puntatore alla location”
  - i file restano nello storage
- Accessi e governance (in scenari reali) sono gestiti via:
  - External Locations / Storage Credentials / Grant (dipende dal setup)

## Cosa include questo repo

- Notebook Databricks:
  - First ingestion + update (append + partizione)
  - Register tables bronze/silver/gold (create metadata)
- Note Power BI sul modello (cartella `powerbi/`)