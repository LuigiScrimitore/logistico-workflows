# Databricks notebook source
# Area: Giacenze
# Layer: Bronze
# Versione: 3.0.0
# Data: 2026-06-08
# Descrizione: Ingestion landing zone -> Delta Bronze per IMBFMOVIM (movimenti magazzino).
#              Sorgente logistix multi-sito (9 siti). MODE=DELTA_MERGE: il file giornaliero
#              contiene il delta; MERGE su chiave naturale (no _bronze_load_date in condizione,
#              no partizione per data).
#              OP-07: path convention pending (struttura Foconi da confermare con Reply).
#              OP-14: IMBFMOVIM (IMF_) e DETTAGLIO_CARR (DTCRL_) sono DUE tabelle distinte.
#              Dato grezzo/unitario: nessun JOIN, nessuna trasformazione (vanno in Silver).

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from utils import get_catalog, add_row_hash, detect_format, read_landing

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from datetime import date
from functools import reduce

# COMMAND ----------
# MAGIC %md #### 1. Widget standard

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")
dbutils.widgets.text("landing_base_path", "abfss://logistica@<storage>.dfs.core.windows.net", "Landing base path")
dbutils.widgets.text("file_format", "auto", "csv | parquet | auto")
dbutils.widgets.text("siti", "lgax,lgcx,lcax,lccx,lexx,locx,lonx,lscx,lslx", "Siti Logistix")

env               = dbutils.widgets.get("env")
run_date          = dbutils.widgets.get("run_date")
landing_base_path = dbutils.widgets.get("landing_base_path").rstrip("/")
file_format       = dbutils.widgets.get("file_format").strip().lower()
siti              = [s.strip() for s in dbutils.widgets.get("siti").split(",") if s.strip()]

# COMMAND ----------
# MAGIC %md #### 2. Parametri specifici del notebook

# COMMAND ----------

NOTEBOOK_NAME = "bronze_movimenti_magazzino"
SOURCE_SYSTEM = "logistix"
TABLE_NAME    = "imbfmovim"
MODE          = "DELTA_MERGE"
IS_MULTISITE  = SOURCE_SYSTEM == "logistix"

# Schema sorgente esplicito (colonne reali verificate - NON modificare/inventare)
SOURCE_COLS = [
    "MAG_SITO_COD", "IMFTIPMOV", "IMFNUMBOL", "IMFANNOBOL", "IMFPRGRIF",
    "IMFCODFOR", "IMFCODIMB", "IMFPREZZO", "IMFQTAADD", "IMFQTAACC",
    "IMFDATMOV", "IMFDATBOL", "IMFDATFAT", "IMFDATCONEF", "IMFNOTE",
    "IMFUSRMAG", "IMFNROCARICO",
]

# MERGE_KEYS - chiave naturale (solo DELTA_MERGE).
# NOTA: le colonne chiave proposte IMF_NRO_MOVIMENTO/IMF_DATA non esistono nello schema reale
# (i campi sono IMF... senza underscore). Si usano le colonne identificative del movimento
# effettivamente presenti: sito + bolla (numero/anno) + progressivo riga + codice imballo.
MERGE_KEYS = [
    "MAG_SITO_COD", "IMFNUMBOL", "IMFANNOBOL", "IMFPRGRIF", "IMFCODIMB",
]

TARGET_CATALOG = get_catalog("bronze", env)
TARGET_SCHEMA  = "logistica"
FULL_TARGET    = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TABLE_NAME}"

logger = get_logger(NOTEBOOK_NAME)
year, month, day = run_date.split("-")

# COMMAND ----------
# MAGIC %md #### 3. Costruzione path landing (logistix-landing)

# COMMAND ----------

def landing_paths():
    """Logistix multi-sito: un path per sito."""
    return [f"{landing_base_path}/logistix-landing/{s}/{TABLE_NAME}/{year}/{month}/{day}/" for s in siti]

def read_one(path):
    fmt = detect_format(path, file_format, dbutils)
    if fmt == "parquet":
        return spark.read.format("parquet").load(path)
    return (spark.read.option("header", "true").option("inferSchema", "false")
            .option("sep", ";").option("encoding", "UTF-8").csv(f"{path}*.csv"))

# COMMAND ----------
# MAGIC %md #### 4. Lettura (unitaria, dato grezzo)

# COMMAND ----------

logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date} | MODE={MODE}")

frames = []
for p in landing_paths():
    try:
        frames.append(read_one(p))
        logger.info(f"Letto: {p}")
    except AnalysisException:
        logger.warning(f"File non trovato: {p} - skip")

if not frames:
    logger.info("Nessun file in landing per la run_date. Notebook terminato.")
    dbutils.notebook.exit("NO_DATA")

raw_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)
raw_df = raw_df.select([c for c in SOURCE_COLS if c in raw_df.columns])

# COMMAND ----------
# MAGIC %md #### 5. Metadati Bronze

# COMMAND ----------

bronze_df = (
    raw_df
    .withColumn("_bronze_load_date", F.lit(run_date).cast("date"))
    .withColumn("_bronze_insert_ts", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)
if IS_MULTISITE:
    bronze_df = bronze_df.withColumn(
        "_sito_cod", F.regexp_extract(F.input_file_name(), r"/logistix-landing/([^/]+)/", 1)
    )

rows_read = bronze_df.count()
logger.info(f"Righe lette: {rows_read}")

# COMMAND ----------
# MAGIC %md #### 6. Scrittura DELTA_MERGE (MERGE su chiave naturale, no partizione)

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
