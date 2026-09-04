# Databricks notebook source
# Area: Preparazione Spedizioni
# Layer: Bronze
# Versione: 3.0.0
# Data: 2026-06-08
# Descrizione: Ingestion giornaliera STORICO_BOLLE (tabella target storico_bolle).
#              Sistema sorgente: STAT  -- MODE: DELTA_MERGE.
#              Riferimento: DOCS/Landing & Bronze - Revision Spec.md
#              Path convention pending OP-07 (struttura Foconi da confermare con Reply).
#
#              OP-16 (CRITICO): STORICO_BOLLE NON proviene da Logistix (lgax) ma dal
#              sistema STAT. Landing a path UNICO (NON multi-sito): stat-landing/storico_bolle.
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

NOTEBOOK_NAME = "bronze_prep_bolle_righe"
SOURCE_SYSTEM = "stat"               # OP-16: sorgente STAT (non Logistix)
TABLE_NAME    = "storico_bolle"
MODE          = "DELTA_MERGE"

MERGE_KEYS    = ["BOL_SITO", "BOL_NRO_BOLLA", "BOL_DATA_BOLLA", "BOL_NRO_RIGA"]

# Schema sorgente esplicito (87 colonne, tutte StringType â€” verificate sul reale, NON modificare)
SOURCE_SCHEMA = StructType([
    StructField("BOL_SITO",                   StringType(), True),
    StructField("BOL_COD_MAGAZZINO",          StringType(), True),
    StructField("BOL_COD_SETTOR_MAG",         StringType(), True),
    StructField("BOL_SEQUE_PRELIEVO",         StringType(), True),
    StructField("BOL_NRO_GABBIA",             StringType(), True),
    StructField("BOL_NRO_ETICHETTA",          StringType(), True),
    StructField("BOL_NRO_CARICO",             StringType(), True),
    StructField("BOL_NRO_ORDINE",             StringType(), True),
    StructField("BOL_PZ_CARTONE",             StringType(), True),
    StructField("BOL_TIPO_PRELIEVO",          StringType(), True),
    StructField("BOL_COD_CARRELLIST",         StringType(), True),
    StructField("BOL_NRO_RIEPILOGO",          StringType(), True),
    StructField("BOL_TIPO_RIEPILOGO",         StringType(), True),
    StructField("BOL_NRO_ORDINE_NEG",         StringType(), True),
    StructField("BOL_TIPO_ORDINE",            StringType(), True),
    StructField("BOL_DATA_ORDIN_NEG",         StringType(), True),
    StructField("BOL_QTA_ORDINE_NEG",         StringType(), True),
    StructField("BOL_COD_NEGOZIO",            StringType(), True),
    StructField("BOL_COD_REPARTO",            StringType(), True),
    StructField("BOL_COD_REPAR_PREP",         StringType(), True),
    StructField("BOL_AREA_NEGOZIO",           StringType(), True),
    StructField("BOL_COD_GESTIONE",           StringType(), True),
    StructField("BOL_TIPO_MOVIMENTO",         StringType(), True),
    StructField("BOL_COD_MSI",                StringType(), True),
    StructField("BOL_COD_EAN",                StringType(), True),
    StructField("BOL_QTA_EVASA",              StringType(), True),
    StructField("BOL_CART_X_STRATO",          StringType(), True),
    StructField("BOL_STRATI_PALLET",          StringType(), True),
    StructField("BOL_CART_X_ULT_ST",          StringType(), True),
    StructField("BOL_DATA_SCADENZA",          StringType(), True),
    StructField("BOL_COD_SOSTITUTO",          StringType(), True),
    StructField("BOL_FLAG_SCARTATO",          StringType(), True),
    StructField("BOL_IVA",                    StringType(), True),
    StructField("BOL_PERC_IVA",               StringType(), True),
    StructField("BOL_COD_CLASS_FISC",         StringType(), True),
    StructField("BOL_CALO_ADDEBITO",          StringType(), True),
    StructField("BOL_CALO_VENDITA",           StringType(), True),
    StructField("BOL_PRZ_ACQ_NETTO",          StringType(), True),
    StructField("BOL_PRZ_ACQ_MEDIO",          StringType(), True),
    StructField("BOL_PRZ_ACQUISTO",           StringType(), True),
    StructField("BOL_PRZ_CESSIONE",           StringType(), True),
    StructField("BOL_PRZ_VENDITA",            StringType(), True),
    StructField("BOL_DATA_PARTENZA",          StringType(), True),
    StructField("BOL_ORA_PARTENZA",           StringType(), True),
    StructField("BOL_COD_AUTISTA",            StringType(), True),
    StructField("BOL_COD_AUTOMEZZO",          StringType(), True),
    StructField("BOL_COD_VETTORE",            StringType(), True),
    StructField("BOL_FLAG_ADDEBITO",          StringType(), True),
    StructField("BOL_DATA_CONSEGNA",          StringType(), True),
    StructField("BOL_NRO_BOLLA",              StringType(), True),
    StructField("BOL_DATA_BOLLA",             StringType(), True),
    StructField("BOL_DATA_BOLLA_DATE",        StringType(), True),
    StructField("BOL_NRO_PGR_STAMPA",         StringType(), True),
    StructField("BOL_FLAG_GIA_STAMP",         StringType(), True),
    StructField("BOL_QTA_DA_EVADERE",         StringType(), True),
    StructField("BOL_DATA_MODIFICA",          StringType(), True),
    StructField("BOL_NOME_UTENTE",            StringType(), True),
    StructField("BOL_DATA_PREPARAZ",          StringType(), True),
    StructField("BOL_NRO_OFFERTA",            StringType(), True),
    StructField("BOL_DATA_FINE_PREP",         StringType(), True),
    StructField("BOL_ORA_FINE_PREP",          StringType(), True),
    StructField("BOL_NRO_RIGA",               StringType(), True),
    StructField("BOL_NRO_ZUCCHERO",           StringType(), True),
    StructField("BOL_FLAG_TRASFERITO",        StringType(), True),
    StructField("BOL_CATENA",                 StringType(), True),
    StructField("BOL_COD_MSI_CATENA",         StringType(), True),
    StructField("BOL_FLAG_TRASFERITO_GOLD",   StringType(), True),
    StructField("BOL_QTA_UF_SPED",            StringType(), True),
    StructField("BOL_MODALITA_PESATA",        StringType(), True),
    StructField("BOL_VALUTA_PREZZI",          StringType(), True),
    StructField("BOL_TIPOMOV",                StringType(), True),
    StructField("BOL_NRO_RIGA_NEG",           StringType(), True),
    StructField("BOL_ORA_CONSEGNA",           StringType(), True),
    StructField("BOL_NRO_SIGILLO",            StringType(), True),
    StructField("BOL_NRO_SIGILLO_RIT",        StringType(), True),
    StructField("BOL_MAG_TRANSITO",           StringType(), True),
    StructField("BOL_SPEDIZIONIERE",          StringType(), True),
    StructField("BOL_DATA_INVIO_SWAP",        StringType(), True),
    StructField("BOL_INDI_STAT",              StringType(), True),
    StructField("BOL_TRASFERITO_STAT",        StringType(), True),
    StructField("BOL_NRO_CONSEGNA",           StringType(), True),
    StructField("BOL_NUM_COSTITUENTI",        StringType(), True),
    StructField("BOL_RIG_ORD",                StringType(), True),
    StructField("BOL_RIF_ORD_GOLD",           StringType(), True),
    StructField("BOL_RIGA_RIGENERATA",        StringType(), True),
    StructField("BOL_DATA_GENERAZIONE_BOLLA", StringType(), True),
    StructField("BOL_DATA_ESTRAZIONE_DWH",    StringType(), True),
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
    # null-safe (<=>): NULL matcha NULL (es. BOL_NRO_RIGA raro null) -> niente append spurio
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

