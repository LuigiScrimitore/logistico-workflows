# Databricks notebook source
# Area: CDT_ESTR (migrazione TO-BE) -> Anagrafiche Vettori
# Layer: Silver
# Versione: 1.0.0
# Data: 2026-06-09
# Descrizione: Replica logica di V_VETTORI (CDT_ESTR) + SP_INS_T_VETTORI (CDT_SA).
#              SORGENTE (RIFATTA): silver.logistica.vettori_track_clean (cleansing del bronze
#              vettori@TRACK), NON più bronze.logistica.wl1_vettori_traspo (staging).
#              DECISIONE FONTE (D §11): il legacy V_VETTORI usa WL1_VETTORI_TRASPO da @TRACK
#              (vettori SORGENTE trasporti); quindi la fonte autoritativa di T_VETTORI è
#              vettori@TRACK, non la VETTORI locale. La copia locale (vettori_locale_clean)
#              resta disponibile per confronto/fallback col functional expert.
#              MODE: FULL_OVERWRITE (anagrafica)
#              Riferimento: CDT_ESTR_VISTE.sql riga 4612 (V_VETTORI); Revisione AS-IS to-be §11.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog

from pyspark.sql import functions as F
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME  = "silver_t_vettori"
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
# RIFATTO: fonte autoritativa = vettori@TRACK pulito (Silver cleansing), non lo staging.
SOURCE_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.vettori_track_clean"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.t_vettori"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    if not spark.catalog.tableExists(SOURCE_TABLE):
        logger.warning(f"Sorgente {SOURCE_TABLE} non esiste. Notebook terminato.")
        dbutils.notebook.exit("NO_SOURCE")

    src = spark.table(SOURCE_TABLE)
    rows_read = src.count()
    logger.info(f"Righe lette da {SOURCE_TABLE}: {rows_read}")
    if rows_read == 0:
        logger.warning("Sorgente vuota. Notebook terminato.")
        dbutils.notebook.exit("NO_DATA")

    check_not_null(src, ["VET_CODICE"], NOTEBOOK_NAME)

    # NB: i nomi VET_* derivano 1:1 da vettori@TRACK (WL1_VETTORI_TRASPO ne era copia pura),
    # quindi il rename resta valido. Da confermare sui dati reali (FASE 1) col functional.
    # V_VETTORI: SELECT VET_CODICE, VET_DESCRIZIONE, ... FROM WL1_VETTORI_TRASPO
    silver_df = (
        src.select(
            F.col("VET_CODICE").cast("string").alias("CODICE_VETTORE"),
            F.col("VET_DESCRIZIONE").cast("string").alias("DESCRIZIONE"),
            F.col("VET_INDIRIZZO").cast("string").alias("INDIRIZZO"),
            F.col("VET_CAP").cast("string").alias("CAP"),
            F.col("VET_CITTA").cast("string").alias("CITTA"),
            F.col("VET_PROVINCIA").cast("string").alias("PROVINCIA"),
            F.col("VET_TELEFONO").cast("string").alias("TELEFONO"),
            F.col("VET_FAX").cast("string").alias("FAX"),
            F.col("VET_EMAIL").cast("string").alias("EMAIL"),
            F.col("VET_MODOLAVORO").cast("string").alias("MODOLAVORO"),
            F.col("VET_NOTE").cast("string").alias("NOTE"),
            F.col("VET_STATO").cast("string").alias("STATO"),
            F.col("VET_TIPO_ORGANIZZATIVO").cast("string").alias("TIPO_ORGANIZZATIVO"),
            F.col("VET_DATA_COST_CONSORZIO").cast("date").alias("DATA_COST_CONSORZIO"),
            F.col("VET_CONSORZIO_RIF").cast("string").alias("CONSORZIO_RIF"),
        )
        .withColumn("_silver_ts", F.current_timestamp())
        .withColumn("_silver_load_date", F.lit(run_date).cast("date"))
    )

    rows_clean = silver_df.count()
    logger.info(f"Righe silver: {rows_clean}")
    check_row_count(silver_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    # FULL_OVERWRITE: anagrafica stato corrente
    (silver_df.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(TARGET_TABLE))
    logger.info(f"FULL OVERWRITE {TARGET_TABLE} ({rows_clean} righe)")

    logger.info(f"END {NOTEBOOK_NAME} | righe_lette={rows_read} | righe_silver={rows_clean}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
