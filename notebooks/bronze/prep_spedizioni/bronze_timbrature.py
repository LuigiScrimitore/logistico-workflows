# Databricks notebook source
# Area: Preparazione Spedizioni
# Layer: Bronze
# Versione: 3.0.0
# Data: 2026-06-08
# Descrizione: Ingestion giornaliera T_PREP_SPED (tabella target t_prep_sped).
#              Sistema sorgente: CND  -- MODE: DELTA_MERGE.
#              Riferimento: DOCS/Landing & Bronze - Revision Spec.md
#              Path convention pending OP-07 (struttura Foconi da confermare con Reply).
#
#              OP-17 (NOTA): T_PREP_SPED e' una tabella GIA' DERIVATA/CONSOLIDATA lato CDT
#              (sistema CND). In Bronze viene acquisita as-is come dato grezzo. Il
#              CONSOLIDAMENTO logico (aggregazioni/derivazioni applicate a monte da CDT)
#              va RICOSTRUITO in Silver a partire dalle sorgenti unitarie, per garantire
#              tracciabilita' e indipendenza dalla logica del sistema legacy.
#
#              Landing a path UNICO (NON multi-sito): cnd-landing/t_prep_sped.
#              Schema sorgente inferito dinamicamente (110+ colonne, tutte StringType):
#              Bronze legge tutte le colonne presenti, Silver selezionera' quelle utili.
#
# MODE DELTA_MERGE: il file giornaliero contiene il delta. MERGE su chiave naturale
#                   (no _bronze_load_date nella condizione, no partizione per data).

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from utils import get_catalog, add_row_hash, detect_format, read_landing

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import date

# COMMAND ----------
# MAGIC %md #### 1. Widget standard

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")
dbutils.widgets.text("landing_base_path", "abfss://logistica@<storage>.dfs.core.windows.net", "Landing base path")
dbutils.widgets.text("file_format", "auto", "csv | parquet | auto")

env               = dbutils.widgets.get("env")
run_date          = dbutils.widgets.get("run_date")
landing_base_path = dbutils.widgets.get("landing_base_path").rstrip("/")
file_format       = dbutils.widgets.get("file_format").strip().lower()

# COMMAND ----------
# MAGIC %md #### 2. Parametri notebook

# COMMAND ----------

NOTEBOOK_NAME = "bronze_timbrature"
SOURCE_SYSTEM = "cnd"                # sorgente CND (path unico, non multi-sito)
TABLE_NAME    = "t_prep_sped"
MODE          = "DELTA_MERGE"

# Chiave naturale (combinazione spedizione/articolo/magazzino).
# Schema non esplicito: la presenza delle MERGE_KEYS e' verificata a runtime sulle colonne reali.
MERGE_KEYS    = ["MAG_SITO_COD", "NUM_RIEP", "SOCIO_COD", "ART_COD"]

TARGET_CATALOG = get_catalog("bronze", env)
TARGET_SCHEMA  = "logistica"
FULL_TARGET    = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TABLE_NAME}"

logger = get_logger(NOTEBOOK_NAME)
year, month, day = run_date.split("-")

# COMMAND ----------
# MAGIC %md #### 3. Path landing (CND — path unico, NON multi-sito)

# COMMAND ----------

base_path = f"{landing_base_path}/{SOURCE_SYSTEM}-landing/{TABLE_NAME}/{year}/{month}/{day}/"

# COMMAND ----------
# MAGIC %md #### 4. Lettura (unitaria, dato grezzo — schema dinamico)

# COMMAND ----------

logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date} | MODE={MODE} | sistema={SOURCE_SYSTEM}")

effective_fmt = detect_format(base_path, file_format, dbutils)

try:
    raw_df = read_landing(spark, base_path, effective_fmt)
    rows_read = raw_df.count()
except Exception as exc:
    logger.warning(f"File non trovato o non leggibile in {base_path}: {exc}")
    dbutils.notebook.exit("NO_DATA")

if rows_read == 0:
    logger.warning(f"Nessuna riga trovata in {base_path}. Notebook terminato.")
    dbutils.notebook.exit("NO_DATA")

logger.info(f"Righe lette: {rows_read} | Colonne: {len(raw_df.columns)} (formato={effective_fmt})")

# Verifica presenza colonne chiave (schema sorgente dinamico, puo' variare)
missing_keys = [k for k in MERGE_KEYS if k not in raw_df.columns]
if missing_keys:
    raise ValueError(f"Colonne MERGE KEY mancanti nel file sorgente: {missing_keys}")

# COMMAND ----------
# MAGIC %md #### 5. Metadati Bronze (no _sito_cod: sorgente CND non multi-sito)

# COMMAND ----------

bronze_df = (
    raw_df
    .withColumn("_bronze_load_date", F.lit(run_date).cast("date"))
    .withColumn("_bronze_insert_ts", F.current_timestamp())
)

# COMMAND ----------
# MAGIC %md #### 6. Scrittura DELTA_MERGE (chiave naturale, no partizione)

# COMMAND ----------

# PRUNING update (OP-30): firma contenuto riga -> propaga solo il delta reale a valle.
bronze_df = add_row_hash(bronze_df)

if not spark.catalog.tableExists(FULL_TARGET):
    bronze_df.write.format("delta").option("mergeSchema", "true").saveAsTable(FULL_TARGET)
    logger.info(f"Creazione iniziale {FULL_TARGET} ({rows_read} righe)")
else:
    cond = " AND ".join(f"tgt.{k} <=> src.{k}" for k in MERGE_KEYS)
    update_set = {c: f"src.{c}" for c in bronze_df.columns
                  if c not in MERGE_KEYS and c != "_bronze_insert_ts"}
    (DeltaTable.forName(spark, FULL_TARGET).alias("tgt")
     .merge(bronze_df.alias("src"), cond)
     .whenMatchedUpdate(condition="tgt._row_hash <> src._row_hash", set=update_set)
     .whenNotMatchedInsertAll()
     .execute())
    logger.info(f"MERGE INTO {FULL_TARGET} completato")

logger.info(f"END {NOTEBOOK_NAME} | righe_processate={rows_read}")
