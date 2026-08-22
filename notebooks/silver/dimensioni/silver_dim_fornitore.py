# Databricks notebook source
# Area: Dimensioni
# Layer: Silver
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: DEPRECATO (OP-02). Dimensione master Fornitore (LU_FORNITORE) fornita dal flusso
#              Retail Master Data. NON va costruita nel layer Silver Logistico: il Gold legge la
#              lookup condivisa in sola lettura. File mantenuto solo per tracciabilita'.

# COMMAND ----------

# DEPRECATO (OP-02): dimensione master fornita dal flusso Retail Master Data (LU_*).
# Non eseguire in produzione: il Gold legge la lookup condivisa in sola lettura.
# Mantenuto per tracciabilita'; rimuovere/ridiscutere a valle della conferma Reply.

dbutils.notebook.exit("DEPRECATED_OP02")
