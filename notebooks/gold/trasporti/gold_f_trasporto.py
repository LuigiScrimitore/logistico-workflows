# Databricks notebook source
# Area: Trasporti — Gold (Fact F_TRASPORTO, grana movimento legacy F_TRASP_MTV)
# Versione: 4.0.0  Data: 2026-06-10
#
# STANDARD 2-NOTEBOOK (Linee guida §1-bis):
#   FASE 3 (normalizzazione) — solo aggancio dimensioni + scrittura.
#   Modellazione (CONS/TRANSITO union) e calcolo sono in silver.logistica_curated.trasporto.
#
# 🔒 REGOLA D'ORO: legge SOLO da silver.logistica_curated.trasporto.
#   Storia: la v3 leggeva silver.logistica.trasporto, derivata da bronze.logistica.t_trasp_mtv
#   (STAGING bronze) — incoerente con scelta B. Ora consuma la catena rebuilt-from-raw
#   (spedizioni_clean + automezzi_clean → s_trasp_mtv → prep), grana legacy F_TRASP_MTV.
#
# Grana = 1 riga per movimento/bolla (CONS o TRANSITO). Partizione: GIORNO_BOLLA_SPED_ID.

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from utils import get_catalog, surrogate_key_fallback
from dq_helper import check_orphan_rate
from pyspark.sql import functions as F
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME  = "gold_f_trasporto"
SILVER_CATALOG = get_catalog("silver", env)
GOLD_CATALOG   = get_catalog("gold",   env)
SOURCE_TABLE   = f"{SILVER_CATALOG}.logistica_curated.trasporto"   # 🔒 unica sorgente
TARGET_TABLE   = f"{GOLD_CATALOG}.logistica.F_TRASPORTO"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    if not spark.catalog.tableExists(SOURCE_TABLE):
        logger.warning(f"Sorgente {SOURCE_TABLE} non esiste. Notebook terminato.")
        dbutils.notebook.exit("NO_SOURCE")

    fact = (spark.read.table(SOURCE_TABLE)
            .withColumn("DWH_UPDATED_AT", F.current_timestamp()))
    rows_src = fact.count()
    logger.info(f"Righe lette da {SOURCE_TABLE}: {rows_src}")
    if rows_src == 0:
        dbutils.notebook.exit("NO_DATA")

    # Chiavi naturali pre-risoluzione surrogate — necessarie per LAD resolver (OP-32).
    fact = (fact
            .withColumn("MAG_SITO_COD_NAT",     F.col("MAG_SITO_COD"))
            .withColumn("VETTORE_SPED_COD_NAT", F.col("VETTORE_SPED_COD")))

    # ── FASE 3: aggancio dimensioni (codice naturale, fallback -1) ──────────────
    LOGISTIC_LU = [
        ("MAG_SITO_COD",     "LU_SITO",     "SITO_COD"),
        ("VETTORE_SPED_COD", "LU_CORRIERE", "CORRIERE_COD"),
    ]
    for fk, lu_tab, lu_pk in LOGISTIC_LU:
        try:
            lu_df = spark.read.table(f"{GOLD_CATALOG}.logistica.{lu_tab}")
            fact = surrogate_key_fallback(fact, fk, lu_df, lu_pk, default_val="-1")
            check_orphan_rate(fact, fk, NOTEBOOK_NAME)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"{lu_tab} non disponibile: aggancio {fk} saltato ({str(e)[:80]})")

    rows = fact.count()
    logger.info(f"F_TRASPORTO righe: {rows}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.logistica")
    (fact.write.format("delta").mode("overwrite").option("partitionOverwriteMode", "dynamic")
        .option("mergeSchema", "true")
        .partitionBy("GIORNO_BOLLA_SPED_ID")
        .saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | righe_scritte={rows} | target={TARGET_TABLE}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
