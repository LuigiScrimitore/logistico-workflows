# Databricks notebook source
# Area: Preparazione Spedizioni
# Layer: Bronze
# Versione: 3.0.0
# Data: 2026-06-08
# Descrizione: Ingestion giornaliera STORICO_RIEPILOGHI (tabella target storico_riepiloghi).
#              Sistema sorgente: STAT  -- MODE: DELTA_MERGE.
#              Riferimento: DOCS/Landing & Bronze - Revision Spec.md
#              Path convention pending OP-07 (struttura Foconi da confermare con Reply).
#
#              OP-16 (CRITICO): STORICO_RIEPILOGHI NON proviene da Logistix (lgax) ma dal
#              sistema STAT. Landing a path UNICO (NON multi-sito): stat-landing/storico_riepiloghi.
#
# MODE DELTA_MERGE: il file giornaliero contiene il delta. MERGE su chiave naturale
#                   (no _bronze_load_date nella condizione, no partizione per data).

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from utils import get_catalog, add_row_hash, detect_format, read_landing

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
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

NOTEBOOK_NAME = "bronze_prep_riepiloghi"
SOURCE_SYSTEM = "stat"               # OP-16: sorgente STAT (non Logistix)
TABLE_NAME    = "storico_riepiloghi"
MODE          = "DELTA_MERGE"

MERGE_KEYS    = ["RPLPR_SITO", "RPLPR_NRO_RIEPILOGO", "RPLPR_DATA_PREPARAZ"]

# Schema sorgente esplicito (47 colonne, tutte StringType â€” verificate sul reale, NON modificare)
SOURCE_SCHEMA = StructType([
    StructField("RPLPR_SITO",                    StringType(), True),
    StructField("RPLPR_COD_MAGAZZINO",           StringType(), True),
    StructField("RPLPR_DATA_PREPARAZ",           StringType(), True),
    StructField("RPLPR_COD_NEGOZIO",             StringType(), True),
    StructField("RPLPR_COD_REPAR_PREP",          StringType(), True),
    StructField("RPLPR_AREA_NEGOZIO",            StringType(), True),
    StructField("RPLPR_COD_SETTOR_MAG",          StringType(), True),
    StructField("RPLPR_NRO_RIEPILOGO",           StringType(), True),
    StructField("RPLPR_TIPO_RIEPILOGO",          StringType(), True),
    StructField("RPLPR_PTY_SCHEDULAZI",          StringType(), True),
    StructField("RPLPR_COD_PREPARATOR",          StringType(), True),
    StructField("RPLPR_FLAG_ESEGUITO",           StringType(), True),
    StructField("RPLPR_DATA_INIZ_PREP",          StringType(), True),
    StructField("RPLPR_ORA_INIZ_PREP",           StringType(), True),
    StructField("RPLPR_DATA_FINE_PREP",          StringType(), True),
    StructField("RPLPR_ORA_FINE_PREP",           StringType(), True),
    StructField("RPLPR_NRO_REFERENZE",           StringType(), True),
    StructField("RPLPR_TOT_CARTONI",             StringType(), True),
    StructField("RPLPR_TOT_QUINTALI",            StringType(), True),
    StructField("RPLPR_NRO_PREPARATI",           StringType(), True),
    StructField("RPLPR_TOT_CART_PREP",           StringType(), True),
    StructField("RPLPR_TOT_QUIN_PREP",           StringType(), True),
    StructField("RPLPR_GABBIE_PREPARA",          StringType(), True),
    StructField("RPLPR_QTA_DA_EV_UDM",           StringType(), True),
    StructField("RPLPR_FLAG_BOLLE",              StringType(), True),
    StructField("RPLPR_NRO_INEVASI",             StringType(), True),
    StructField("RPLPR_TOT_CART_INEV",           StringType(), True),
    StructField("RPLPR_TOT_QUIN_INEV",           StringType(), True),
    StructField("RPLPR_GABBIE_TRATT",            StringType(), True),
    StructField("RPLPR_NRO_GABBIA_MIN",          StringType(), True),
    StructField("RPLPR_FLAG_INIZIATO",           StringType(), True),
    StructField("RPLPR_FLAG_FINITO",             StringType(), True),
    StructField("RPLPR_NRO_COMMISSIONE",         StringType(), True),
    StructField("RPLPR_NOME_UTENTE",             StringType(), True),
    StructField("RPLPR_DATA_MODIFICA",           StringType(), True),
    StructField("RPLPR_PL_INT_GENERATI",         StringType(), True),
    StructField("RPLPR_PORTA_USCITA",            StringType(), True),
    StructField("RPLPR_COD_ZONA_MAG",            StringType(), True),
    StructField("RPLPR_GITA",                    StringType(), True),
    StructField("RPLPR_COD_AREA_MERCEOLOGICA",   StringType(), True),
    StructField("RPLPR_TRASFERITO_STAT",         StringType(), True),
    StructField("RPLPR_VOLUME",                  StringType(), True),
    StructField("RPLPR_NOME_TERMINALE",          StringType(), True),
    StructField("RPLPR_CARRELLO",                StringType(), True),
    StructField("RPLPR_DATA_INSERIMENTO",        StringType(), True),
    StructField("RPLPR_TIPO_IMBALLO",            StringType(), True),
    StructField("RPLPR_DATA_ESTRAZIONE_DWH",     StringType(), True),
])

TARGET_CATALOG = get_catalog("bronze", env)
TARGET_SCHEMA  = "logistica"
FULL_TARGET    = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TABLE_NAME}"

logger = get_logger(NOTEBOOK_NAME)
year, month, day = run_date.split("-")

# COMMAND ----------
# MAGIC %md #### 3. Path landing (STAT â€” path unico, NON multi-sito)

# COMMAND ----------

base_path = f"{landing_base_path}/{SOURCE_SYSTEM}-landing/{TABLE_NAME}/{year}/{month}/{day}/"

# COMMAND ----------
# MAGIC %md #### 4. Lettura (unitaria, dato grezzo)

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

logger.info(f"Righe lette: {rows_read} (formato={effective_fmt})")

# COMMAND ----------
# MAGIC %md #### 5. Metadati Bronze (no _sito_cod: sorgente STAT non multi-sito)

# COMMAND ----------

bronze_df = (
    raw_df
    .withColumn("_bronze_load_date", F.lit(run_date).cast("date"))
    .withColumn("_bronze_insert_ts", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
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

