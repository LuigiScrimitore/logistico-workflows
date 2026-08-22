# Databricks notebook source
# Area: Preparazione Spedizioni
# Layer: Bronze
# Versione: 1.0.0
# Data: 2026-06-09
# Descrizione: Ingestion 1:1 della sorgente RAW STORICO_LISTE (sistema STAT) verso Bronze Delta.
#              Ridisegno SCELTA B: si legge la VERA sorgente raw (stat-landing/storico_liste),
#              NON piu' lo staging wl1_storico_liste/_uniche. L'elaborazione UNICHE e' replicata
#              nei Silver, non letta dallo staging.
#              MODE = DELTA_MERGE (finestra data nel landing, no flag CDC).
#              Schema-on-read (StringType) â€” nessuna derivata, nessun join (Bronze 1:1 puro).
#              Riferimento: config landing simulator (systems.stat.storico_liste); Â§11 catalogo RAW.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from utils import get_catalog, detect_format, read_landing

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

NOTEBOOK_NAME = "bronze_storico_liste"
SOURCE_SYSTEM = "stat"               # sorgente STAT (path unico, non multi-sito)
TABLE_NAME    = "storico_liste"
MODE          = "DELTA_MERGE"

# Chiave naturale = 8 chiavi di prelievo (allineata al landing config).
MERGE_KEYS    = [
    "LSPRL_SITO", "LSPRL_NRO_GABBIA", "LSPRL_NRO_ORDINE_NEG", "LSPRL_COD_NEGOZIO",
    "LSPRL_COD_MSI", "LSPRL_DATA_ORDIN_NEG", "LSPRL_SEQUE_PRELIEVO", "LSPRL_FLAG_SCARTATO",
]

# Schema-on-read: SELECT * (tutte StringType). Nessuna colonna esplicita per non vincolare
# lo schema reale della sorgente raw (verra' fissato dopo la prima estrazione reale).
SOURCE_COLS = []

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
# PRUNING update: firma del contenuto riga (solo colonne business, non i metadati _*).
# Coalesce con sentinella per non confondere null e stringa vuota.
_biz_cols = [c for c in bronze_df.columns if not c.startswith("_")]
bronze_df = bronze_df.withColumn(
    "_row_hash",
    F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in _biz_cols]), 256),
)

# COMMAND ----------
# MAGIC %md #### 6. Scrittura DELTA_MERGE (chiave naturale 8 chiavi, no partizione)

# COMMAND ----------

# Schema-evolution sul MERGE: assorbe eventuali colonne nuove del sorgente
# (es. LSPRL_COD_ESTERNO_ORDINE) senza rompere whenMatchedUpdate/InsertAll.
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

if not spark.catalog.tableExists(FULL_TARGET):
    bronze_df.write.format("delta").option("mergeSchema", "true").saveAsTable(FULL_TARGET)
    logger.info(f"Creazione iniziale {FULL_TARGET} ({rows_read} righe)")
else:
    # null-safe (<=>) sulle chiavi (alcune nullable, es. SEQUE_PRELIEVO).
    cond = " AND ".join(f"tgt.{k} <=> src.{k}" for k in MERGE_KEYS)
    update_set = {c: f"src.{c}" for c in bronze_df.columns
                  if c not in MERGE_KEYS and c != "_bronze_insert_ts"}
    # PRUNING: aggiorna (e ri-data _bronze_load_date) SOLO se il contenuto e' cambiato.
    # Riga identica (_row_hash uguale) -> nessun update -> _bronze_load_date resta vecchia
    # -> NON propagata a valle (clean/prep incrementale la ignora). Delta reale, non finestra.
    (DeltaTable.forName(spark, FULL_TARGET).alias("tgt")
     .merge(bronze_df.alias("src"), cond)
     .whenMatchedUpdate(condition="tgt._row_hash <> src._row_hash", set=update_set)
     .whenNotMatchedInsertAll()
     .execute())
    logger.info(f"MERGE INTO {FULL_TARGET} completato (pruning by _row_hash)")

logger.info(f"END {NOTEBOOK_NAME} | righe_processate={rows_read}")

