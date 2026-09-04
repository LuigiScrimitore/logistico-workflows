# Databricks notebook source
# DEPRECATO (OP-02): dimensione master fornita dal flusso Retail Master Data (LU_ART_RADICE).
# Non eseguire in produzione: il Gold legge la lookup condivisa in sola lettura dallo schema
# Retail Master (parametro retail_master_schema, default gold_prod.condiviso in attesa OP-02).
# Mantenuto per tracciabilità; rimuovere/ridiscutere a valle della conferma Reply.
#
# Versione: 3.0.0  Data: 2026-06-08
# Riferimento: DOCS/Gold - Revision Spec.md (sez. 2) + DOCS/Open Points OP-02

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

dbutils.notebook.exit("DEPRECATED_OP02")
