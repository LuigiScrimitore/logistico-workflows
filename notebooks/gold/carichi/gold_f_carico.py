# Databricks notebook source
# Area: Carichi — Gold (Fact F_CARICO)
# Versione: 4.0.0  Data: 2026-06-10
# Grain: 1 riga = riga dettaglio carico (testata x dettaglio).
#
# STANDARD 2-NOTEBOOK (Linee guida §1-bis):
#   FASE 3 (normalizzazione) — solo aggancio dimensioni + scrittura.
#   Modellazione (testata⋈dettaglio⋈pesata) e calcolo (SCARTO_QTA, ANNO_MESE) sono in
#   silver.logistica_curated.carico.
#
# 🔒 REGOLA D'ORO: legge SOLO da silver.logistica_curated.carico.
#   NB: il vettore del carico (CORRIERE_COD <- STCAR_COD_CORRIERE) e' agganciato a LU_CORRIERE.
#   Verifica 2026-07-01: CDT_DW.F_CARICO.VETTORE_CARICO_COD e' popolato al 100% (l'assunzione
#   precedente "0 occorrenze" era errata) -> il vettore inbound va tenuto.

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from utils import get_catalog, attach_carico_dimensions, attach_carico_peso_volume
from pyspark.sql import functions as F
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")
dbutils.widgets.text("retail_master_schema", "bronze_dev.condiviso",
                     "Schema lookup master (workaround CDT_DW; OP-02 -> gold_prod.condiviso)")

env       = dbutils.widgets.get("env")
run_date  = dbutils.widgets.get("run_date")
retail_ms = dbutils.widgets.get("retail_master_schema").strip()

# COMMAND ----------

NOTEBOOK_NAME  = "gold_f_carico"
SILVER_CATALOG = get_catalog("silver", env)
GOLD_CATALOG   = get_catalog("gold",   env)
SOURCE_TABLE   = f"{SILVER_CATALOG}.logistica_curated.carico"   # 🔒 unica sorgente
TARGET_TABLE   = f"{GOLD_CATALOG}.logistica.F_CARICO"

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

    # ── FASE 3: colonne _NAT + aggancio anagrafiche (logica condivisa) ──────────
    fact = attach_carico_dimensions(spark, fact, GOLD_CATALOG, retail_ms, logger, NOTEBOOK_NAME)

    # ── FASE 3b: PES_CARICO/VOL_CARICO da LU_ART_UNITA_LOGISTICA (formula ODI) ───
    fact = attach_carico_peso_volume(spark, fact, retail_ms, logger, NOTEBOOK_NAME)

    rows = fact.count()
    logger.info(f"F_CARICO righe: {rows}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.logistica")
    (fact.write.format("delta").mode("overwrite").option("partitionOverwriteMode", "dynamic")
        .option("mergeSchema", "true")
        .partitionBy("ANNO_MESE")
        .saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | righe_scritte={rows} | target={TARGET_TABLE}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
