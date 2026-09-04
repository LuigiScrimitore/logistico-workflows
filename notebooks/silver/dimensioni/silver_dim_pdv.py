# Databricks notebook source
# Area: Dimensioni
# Layer: Silver
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: DEPRECATO (OP-02). Dimensione master PDV (LU_PDV) fornita dal flusso Retail Master
#              Data. NON va costruita nel layer Silver Logistico: il Gold legge la lookup condivisa
#              in sola lettura. Nota: bronze.logistica.t_pdv (CND) resta disponibile per eventuali
#              attributi logistici (OP-05). File mantenuto solo per tracciabilita'.

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

# DEPRECATO (OP-02): dimensione master fornita dal flusso Retail Master Data (LU_*).
# Non eseguire in produzione: il Gold legge la lookup condivisa in sola lettura.
# Mantenuto per tracciabilita'; rimuovere/ridiscutere a valle della conferma Reply.

dbutils.notebook.exit("DEPRECATED_OP02")
