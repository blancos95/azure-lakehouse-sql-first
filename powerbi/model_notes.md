# Power BI — Model notes (Gold layer)

Queste note descrivono come consumare il layer **Gold** da Power BI in un’architettura Lakehouse su Azure + Databricks.

## Connessione Power BI ↔ Databricks (pattern tipico)

Opzioni comuni:
1) **Databricks SQL Warehouse / Serverless SQL**
   - Power BI si connette al warehouse (endpoint SQL).
   - Consigliato per scenari BI “classici”.

2) **Databricks Cluster (con connector)**
   - Utile per test o ambienti non standard, ma meno “enterprise” rispetto al Warehouse.

> Nel portfolio: assumiamo consumo via **Databricks SQL Warehouse**.

## Import vs DirectQuery

### Import (consigliato per dashboard standard)
- Performance migliori lato report
- Supporta **Incremental Refresh**
- Ideale se i dati in Gold non cambiano ogni minuto

### DirectQuery (quando serve quasi-real-time)
- Query live verso Databricks
- Dipende molto dalle performance del warehouse e dalle viste/tabella Gold
- Richiede attenzione a modellazione e misure

## Gold dataset: cosa esporre a Power BI

Pattern consigliato: pubblicare in Gold tabelle “BI-ready” (pulite, con naming business).

Esempio:
- `gold.nometabella_actual` → fotografia corrente (una riga per chiave)
- `gold.nometabella_scd2` → storico (SCD Type 2: valid_from, valid_to, is_current)

## Naming e best practice (Gold)

- Colonne con nomi “business friendly” (evitare abbreviazioni tecniche)
- Tipi dati già corretti (date/decimal/boolean)
- Evitare calcoli pesanti in Power BI se puoi farli in Gold (SQL/Delta)

## Misure DAX di esempio (portfolio)

Sostituisci `nometabella_actual` e i nomi colonna con quelli reali.

```DAX
-- 1) Righe totali
Rows =
COUNTROWS ( 'nometabella_actual' )

-- 2) Totale importo (se esiste una colonna amount)
Total Amount =
SUM ( 'nometabella_actual'[amount] )

-- 3) Media importo
Avg Amount =
AVERAGE ( 'nometabella_actual'[amount] )

-- 4) Righe per data (se esiste una colonna data tipo dp)
Rows by Date =
CALCULATE (
    [Rows],
    ALLEXCEPT ( 'nometabella_actual', 'nometabella_actual'[dp] )
)