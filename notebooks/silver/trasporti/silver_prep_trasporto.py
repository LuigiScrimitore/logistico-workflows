# Databricks notebook source
# Area: Trasporti — Fact trasporto (GRANA BOLLA)
# Layer: Silver PREP (Fase 1 modellazione + Fase 2 calcolo)  →  silver.logistica_curated.trasporto
# Versione: 3.0.0
# Data: 2026-06-10
# Descrizione: STRATO PREP (standard 2-notebook §1-bis). RIDISEGNO grana BOLLA (decisione 2026-06-10):
#              la sorgente viva SPEDIZIONI@TRACK e' a grana bolla (1 riga per spedizione).
#              La vecchia grana-movimento (azioni C/S/RP/RT, ex S_TRASP_MTV) NON e' piu'
#              alimentabile (S_TRASP_MTV dismessa) -> dismesso silver_trasp_mtv_build.
#
#              SORGENTE: silver.logistica.spedizioni_clean (cleansing dei campi SP_*).
#              FASE 1: nessun join (single-source bolla).
#              FASE 2: mappatura SP_* -> nomi fact, chiavi-giorno clean_dat_d, LEAD_TIME_GG,
#                      flag transito. Surrogate key dimensionali nel Gold (Fase 3).
#              Grana = SP_ID (1 bolla). MODE: DELTA_MERGE su SP_ID.

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog, clean_dat_d

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME  = "silver_prep_trasporto"
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.spedizioni_clean"   # grana bolla (SP_*)
TARGET_TABLE   = f"{SILVER_CATALOG}.logistica_curated.trasporto"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    if not spark.catalog.tableExists(SOURCE_TABLE):
        logger.warning(f"Sorgente {SOURCE_TABLE} non esiste. Notebook terminato.")
        dbutils.notebook.exit("NO_SOURCE")

    sp = spark.table(SOURCE_TABLE)
    rows_read = sp.count()
    logger.info(f"Righe lette da {SOURCE_TABLE}: {rows_read}")
    if rows_read == 0:
        dbutils.notebook.exit("NO_DATA")

    # ── FASE 2: mappatura SP_* -> schema fact (grana bolla) ────────────────────
    has_transito = (F.coalesce(F.col("SP_MAGTRANSITO").cast("string"), F.lit("")) != "") & \
                   (F.coalesce(F.col("SP_MAGTRANSITO").cast("string"), F.lit("0")) != "0")

    prep_df = sp.select(
        F.col("SP_ID").alias("SP_ID"),
        F.col("SP_MAGAZZINO").alias("MAG_SITO_COD"),
        F.col("SP_MAGAZZINO").alias("TRASP_NODO_ORIG_COD"),
        F.col("SP_PDV").alias("TRASP_NODO_DEST_COD"),
        F.col("SP_MAGTRANSITO").alias("MAG_SITO_TRANSITO_COD"),
        has_transito.cast("int").alias("MAG_SITO_TRANSITO_FLAG"),
        # 1 = CEDI->CEDI (transito), 2 = CEDI->PDV (diretto)
        F.when(has_transito, F.lit(1)).otherwise(F.lit(2)).alias("TRASP_TIPO_ID"),
        F.col("SP_VETTORE").alias("VETTORE_SPED_COD"),
        F.col("SP_AUTISTA").alias("AUTISTA_SPED_COD"),
        F.col("SP_AUTOMEZZO").alias("AUTOM_SPED_COD"),
        F.col("SP_TARGA").alias("AUTOM_TARGA"),
        F.col("SP_NUMBOLLA").alias("NUM_BOLLA_SPED"),
        F.col("SP_DATABOLLA").cast("date").alias("DATA_BOLLA_SPED"),
        clean_dat_d(F.col("SP_DATABOLLA")).alias("GIORNO_BOLLA_SPED_ID"),
        clean_dat_d(F.col("SP_DATASPED")).alias("GIORNO_SPED_ID"),
        clean_dat_d(F.col("SP_DATACONSPREV")).alias("GIORNO_PREV_CONS_SOCIO_ID"),
        clean_dat_d(F.col("SP_DATASCARICO")).alias("GIORNO_CONS_SOCIO_ID"),
        clean_dat_d(F.col("SP_DATAPRESACARICO")).alias("GIORNO_PRESA_CARICO_ID"),
        F.col("SP_STATO").alias("STATO_SPED_COD"),
        F.col("SP_TIPO").alias("TRASP_TIPO_COD"),
        F.col("SP_TIPO_BOLLA").alias("TIPO_BOLLA_COD"),
        F.col("SP_ID_VIAGGIO").alias("NUM_VIAGGIO"),
        F.col("SP_ID_DISTINTA").alias("NUM_DISTINTA"),
        F.col("SP_NUMSIGILLO").alias("NUM_SIGILLO"),
        F.col("SP_NUMSIGILLORIT").alias("NUM_SIGILLO_RIT"),
        # LEAD_TIME_GG = giorni tra bolla e scarico effettivo (consegna)
        F.datediff(F.col("SP_DATASCARICO").cast("date"), F.col("SP_DATABOLLA").cast("date")).alias("LEAD_TIME_GG"),
    ).withColumn("_silver_ts", F.current_timestamp()) \
     .withColumn("_silver_load_date", F.lit(run_date).cast("date"))

    check_not_null(prep_df, ["SP_ID", "MAG_SITO_COD", "NUM_BOLLA_SPED"], NOTEBOOK_NAME)
    rows_clean = prep_df.count()
    logger.info(f"Righe prep trasporto (grana bolla): {rows_clean}")
    check_row_count(prep_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_CATALOG}.logistica_curated")

    if not spark.catalog.tableExists(TARGET_TABLE):
        logger.info(f"Prima esecuzione — CTAS {TARGET_TABLE}")
        prep_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)
    else:
        DeltaTable.forName(spark, TARGET_TABLE).alias("tgt").merge(
            prep_df.alias("src"), "tgt.SP_ID = src.SP_ID"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.info(f"MERGE INTO {TARGET_TABLE} completato")

    logger.info(f"END {NOTEBOOK_NAME} | righe_lette={rows_read} | righe_prep={rows_clean}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
