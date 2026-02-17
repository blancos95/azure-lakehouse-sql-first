# SCD2 (Gold) — Note operative (portfolio)

Questa sezione descrive il pattern **Slowly Changing Dimension Type 2 (SCD2)** usato in Gold per tracciare lo storico delle modifiche. :contentReference[oaicite:1]{index=1}

## Concetto base

- Tabella **ACTUAL**: contiene sempre lo stato più aggiornato, viene tipicamente **sovrascritta** a ogni run (snapshot corrente). :contentReference[oaicite:2]{index=2}
- Tabella **SCD2**: mantiene lo storico. Ogni record ha:
  - `record_validity_from_date` = data inizio validità
  - `record_validity_to_date` = data fine validità (di default `'9999-01-01'` per record “aperti”) :contentReference[oaicite:3]{index=3}

Quando un record cambia:
1) la versione precedente viene **chiusa** (to_date = data del cambiamento)
2) viene inserita una **nuova riga** con i nuovi valori (from_date = data del cambiamento, to_date = `'9999-01-01'`). 

## Come si interroga “lo stato attuale” dalla SCD2

Per ottenere solo i record attivi (aperti), tipicamente:
- `record_validity_to_date = '9999-01-01'`
oppure
- `record_validity_to_date > current_date()` :contentReference[oaicite:5]{index=5}

## Pattern di esecuzione (Databricks)

Input:
- `df_t0`: SCD2 corrente (prima dell’update)
- `df_t1`: ACTUAL del giorno (snapshot) 

Controllo fondamentale:
- se `df_t1` è vuoto, **skippare l’esecuzione** per evitare di chiudere erroneamente tutti i record. 

Pattern “safe write”:
- scrivere prima in tabella **backup**
- poi sovrascrivere la tabella SCD2 finale leggendo dalla backup 

## Nota “portfolio”
Nel repo è presente un notebook template che simula la logica SCD2 (inizializzazione + update) in modo semplificato e riutilizzabile (placeholder).
