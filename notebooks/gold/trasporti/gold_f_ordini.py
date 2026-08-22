# Databricks notebook source
# Area: Trasporti / Ordini — Gold (Fact F_ORDINI)
# Versione: 4.0.0  Data: 2026-06-10
# Grain: 1 riga = ordine/carico (testata).
#
# STANDARD 2-NOTEBOOK (Linee guida §1-bis):
#   FASE 3 (normalizzazione) — solo aggancio dimensioni + scrittura.
#   Modellazione/calcolo (ANNO_MESE, GIORNO_CARICO_ID) sono in silver.logistica_curated.ordini.
#
# 🔒 REGOLA D'ORO: legge SOLO da silver.logistica_curated.ordini.
#   NOTA: la v3 leggeva silver.logistica.ordine e NON faceva alcun aggancio dimensionale
#   (incoerente con gli altri fatti). Qui aggiungiamo fornitore/corriere/sito (fallback -1).
#   La testata NON contiene quantita' (sono nel dettaglio = F_CARICO). Stato = PROXY FLAG_TRASFERITO.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from utils import get_catalog, surrogate_key_fallback
from dq_helper import check_orphan_rate
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

NOTEBOOK_NAME  = "gold_f_ordini"
SILVER_CATALOG = get_catalog("silver", env)
GOLD_CATALOG   = get_catalog("gold",   env)
SOURCE_TABLE   = f"{SILVER_CATALOG}.logistica_curated.ordini"   # 🔒 unica sorgente
TARGET_TABLE   = f"{GOLD_CATALOG}.logistica.F_ORDINI"

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
            .withColumn("FORNITORE_COD_NAT", F.col("FORNITORE_COD"))
            .withColumn("SITO_COD_NAT",      F.col("SITO_COD")))

    # ── FASE 3: aggancio dimensioni (codice naturale, fallback -1) ──────────────
    # Fornitore: anagrafica retail condivisa.
    try:
        lu_forn = spark.read.table(f"{retail_ms}.LU_FORNITORE")
        fact = surrogate_key_fallback(fact, "FORNITORE_COD", lu_forn, "FORN_COD", default_val="-1")
        check_orphan_rate(fact, "FORNITORE_COD", NOTEBOOK_NAME)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"LU_FORNITORE non disponibile in {retail_ms}: aggancio fornitore saltato ({str(e)[:80]})")

    # Anagrafiche logistiche proprietarie.
    # NB: niente LU_CORRIERE — gli ordini/carichi sono INBOUND e non hanno dimensione corriere
    # (STCAR_COD_CORRIERE risulta sempre nullo; coerente con la scelta su F_CARICO, OP/task #30).
    # Il corriere e' dimensione di spedizione/trasporto (F_PREP_SPED / F_TRASPORTO).
    LOGISTIC_LU = [
        ("SITO_COD",     "LU_SITO",     "SITO_COD"),
    ]
    for fk, lu_tab, lu_pk in LOGISTIC_LU:
        try:
            lu_df = spark.read.table(f"{GOLD_CATALOG}.logistica.{lu_tab}")
            fact = surrogate_key_fallback(fact, fk, lu_df, lu_pk, default_val="-1")
            check_orphan_rate(fact, fk, NOTEBOOK_NAME)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"{lu_tab} non disponibile: aggancio {fk} saltato ({str(e)[:80]})")

    rows = fact.count()
    logger.info(f"F_ORDINI righe: {rows}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.logistica")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (fact.write.format("delta").mode("overwrite")
        .option("mergeSchema", "true")
        .partitionBy("ANNO_MESE")
        .saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | righe_scritte={rows} | target={TARGET_TABLE}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
