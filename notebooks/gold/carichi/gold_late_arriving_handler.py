# Databricks notebook source
# Area: Carichi — Gold (Fact — Late Arriving Handler)
# Versione: 4.0.0  Data: 2026-07-01
# Descrizione: Riprocessa F_CARICO per le partizioni ANNO_MESE PASSATE (DATA_CARICO < inizio mese corrente)
#              e finestra di look-back (default 90 giorni). Per ciascun ANNO_MESE distinto trovato
#              ricostruisce la fact con replaceWhere su quella partizione.
#
# 🔒 REGOLA D'ORO: legge SOLO da silver.logistica_curated.carico (stessa sorgente di gold_f_carico).
#   FASE 3 (aggancio anagrafiche + colonne _NAT) IDENTICA a gold_f_carico -> lo schema della
#   partizione riprocessata resta allineato al resto della tabella (fix OP-32/LAD resolver).

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from utils import get_catalog, attach_carico_dimensions, attach_carico_peso_volume
from pyspark.sql import functions as F
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")
dbutils.widgets.text("lookback_days", "90", "Finestra look-back per late-arriving (giorni)")
dbutils.widgets.text("retail_master_schema", "bronze_dev.condiviso",
                     "Schema lookup master (workaround CDT_DW; OP-02 -> gold_prod.condiviso)")

env       = dbutils.widgets.get("env")
run_date  = dbutils.widgets.get("run_date")
look_days = int(dbutils.widgets.get("lookback_days"))
retail_ms = dbutils.widgets.get("retail_master_schema").strip()

NOTEBOOK_NAME  = "gold_late_arriving_handler"
SILVER_CATALOG = get_catalog("silver", env)
GOLD_CATALOG   = get_catalog("gold",   env)
SOURCE_TABLE   = f"{SILVER_CATALOG}.logistica_curated.carico"   # 🔒 unica sorgente (come gold_f_carico)
TARGET_TABLE   = f"{GOLD_CATALOG}.logistica.F_CARICO"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date} | lookback={look_days}gg")

    if not spark.catalog.tableExists(SOURCE_TABLE):
        logger.warning(f"Sorgente {SOURCE_TABLE} non esiste. Notebook terminato.")
        dbutils.notebook.exit("NO_SOURCE")

    # 1) Finestra late-arriving: carichi con DATA_CARICO PRECEDENTE al mese corrente
    start_curr_month = F.trunc(F.lit(run_date).cast("date"), "month")
    lookback_start = F.date_sub(F.lit(run_date).cast("date"), look_days)

    fact = (spark.read.table(SOURCE_TABLE)
            .filter(F.col("DATA_CARICO") >= lookback_start)
            .filter(F.col("DATA_CARICO") < start_curr_month)
            .withColumn("DWH_UPDATED_AT", F.current_timestamp()))

    if fact.rdd.isEmpty():
        logger.info("Nessun late-arriving da processare nella finestra.")
        dbutils.notebook.exit("NO_LATE_ARRIVING")

    # 2) Distinct ANNO_MESE coinvolti
    months = [r[0] for r in fact.select(F.col("ANNO_MESE"))
                              .distinct().orderBy("ANNO_MESE").collect()]
    logger.info(f"ANNO_MESE da riprocessare: {months}")

    # ── FASE 3: colonne _NAT + aggancio anagrafiche (logica condivisa) ──────────
    #    Stessa funzione di gold_f_carico -> parità di schema garantita tra flusso
    #    normale e late-arriving (colonne _NAT + surrogate agganciati).
    fact = attach_carico_dimensions(spark, fact, GOLD_CATALOG, retail_ms, logger, NOTEBOOK_NAME)
    fact = attach_carico_peso_volume(spark, fact, retail_ms, logger, NOTEBOOK_NAME)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.logistica")
    # 3) Una replaceWhere per ciascun ANNO_MESE coinvolto (idempotente per partizione)
    for am in months:
        part = fact.filter(F.col("ANNO_MESE") == am)
        n = part.count()
        logger.info(f"Late-arriving ANNO_MESE={am} righe={n}")
        (part.write.format("delta").mode("overwrite")
            .option("replaceWhere", f"ANNO_MESE = '{am}'")
            .option("mergeSchema", "true")
            .partitionBy("ANNO_MESE")
            .saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | partizioni_riprocessate={len(months)} | target={TARGET_TABLE}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
