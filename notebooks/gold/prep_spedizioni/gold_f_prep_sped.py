# Databricks notebook source
# Area: Preparazione Spedizioni — Gold (Fact)
# Versione: 4.0.0  Data: 2026-06-10
# Tabella: F_PREP_SPED  (grana = prelievo articolo, legacy-fedele a CDT_DW.f_prep_sped)
#
# STANDARD 2-NOTEBOOK (Linee guida §1-bis):
#   FASE 3 (normalizzazione) — questo notebook fa SOLO aggancio dimensioni + scrittura.
#   Modellazione (join) e calcolo (SEC_PREP_PREL, valorizzazioni) sono in silver.logistica_curated.prep_sped.
#
# 🔒 REGOLA D'ORO: legge SOLO da silver.logistica_curated.prep_sped.
#                  (Storia: la v3 leggeva prep_riepilogo STAT = grana produttivita'/turno,
#                   che NON e' il F_PREP_SPED legacy. Quella logica e' migrata a F_TURNO_PREP_SITO.)
#
# Aggancio dimensioni per codice naturale con surrogate_key_fallback(-1) + check_orphan_rate.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from utils import get_catalog, surrogate_key_fallback
from dq_helper import check_orphan_rate
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
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

NOTEBOOK_NAME  = "gold_f_prep_sped"
SILVER_CATALOG = get_catalog("silver", env)
GOLD_CATALOG   = get_catalog("gold",   env)
SOURCE_TABLE   = f"{SILVER_CATALOG}.logistica_curated.prep_sped"   # 🔒 unica sorgente
TARGET_TABLE   = f"{GOLD_CATALOG}.logistica.F_PREP_SPED"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    if not spark.catalog.tableExists(SOURCE_TABLE):
        logger.warning(f"Sorgente {SOURCE_TABLE} non esiste. Notebook terminato.")
        dbutils.notebook.exit("NO_SOURCE")

    src = spark.read.table(SOURCE_TABLE)
    rows_src = src.count()
    logger.info(f"Righe lette da {SOURCE_TABLE}: {rows_src}")
    if rows_src == 0:
        logger.warning("Sorgente prep_sped vuota. Notebook terminato.")
        dbutils.notebook.exit("NO_DATA")

    # Colonna-data per partizione (dal timestamp inizio prelievo).
    fact = (src
            .withColumn("DATA_PREL", F.col("DATA_PREL_INIZ").cast(DateType()))
            .withColumn("PDV_COD", F.col("SOCIO_COD"))   # socio = PDV destinatario (codice naturale)
            .withColumn("DWH_UPDATED_AT", F.current_timestamp()))

    # Chiavi naturali pre-risoluzione surrogate — necessarie per LAD resolver (OP-32).
    # PDV_COD_NAT = SOCIO_COD originale (rinominato sopra nella costruzione di fact).
    fact = (fact
            .withColumn("PDV_COD_NAT",                F.col("PDV_COD"))
            .withColumn("MAG_SITO_COD_NAT",           F.col("MAG_SITO_COD"))
            .withColumn("ART_RADICE_COD_NAT",         F.col("ART_RADICE_COD"))
            .withColumn("OPER_PREP_COD_NAT",          F.col("OPER_PREP_COD"))
            .withColumn("VETTORE_PRESU_SPED_COD_NAT", F.col("VETTORE_PRESU_SPED_COD")))

    # ── FASE 3: aggancio dimensioni ─────────────────────────────────────────────
    # Anagrafica retail condivisa (PDV) per codice naturale.
    try:
        lu_pdv = spark.read.table(f"{retail_ms}.LU_PDV")
        fact = surrogate_key_fallback(fact, "PDV_COD", lu_pdv, "PDV_COD", default_val="-1")
        check_orphan_rate(fact, "PDV_COD", NOTEBOOK_NAME)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"LU_PDV non disponibile in {retail_ms}: aggancio PDV saltato ({str(e)[:80]})")

    # Anagrafica articolo radice (retail condivisa) per codice naturale.
    try:
        lu_art = spark.read.table(f"{retail_ms}.LU_ART_RADICE")
        fact = surrogate_key_fallback(fact, "ART_RADICE_COD", lu_art, "ART_RADICE_COD", default_val="-1")
        check_orphan_rate(fact, "ART_RADICE_COD", NOTEBOOK_NAME)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"LU_ART_RADICE non disponibile in {retail_ms}: aggancio articolo saltato ({str(e)[:80]})")

    # Anagrafiche logistiche proprietarie (gold_prod.logistica), fallback -1.
    LOGISTIC_LU = [
        ("MAG_SITO_COD",            "LU_SITO",       "SITO_COD"),
        ("OPER_PREP_COD",           "LU_OPERATORE",  "OPERATORE_COD"),
        ("VETTORE_PRESU_SPED_COD",  "LU_CORRIERE",   "CORRIERE_COD"),
    ]
    for fk, lu_tab, lu_pk in LOGISTIC_LU:
        try:
            lu_df = spark.read.table(f"{GOLD_CATALOG}.logistica.{lu_tab}")
            fact = surrogate_key_fallback(fact, fk, lu_df, lu_pk, default_val="-1")
            check_orphan_rate(fact, fk, NOTEBOOK_NAME)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"{lu_tab} non disponibile: aggancio {fk} saltato ({str(e)[:80]})")

    rows = fact.count()
    logger.info(f"F_PREP_SPED righe: {rows}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.logistica")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (fact.write.format("delta").mode("overwrite")
        .option("mergeSchema", "true")
        .partitionBy("DATA_PREL")
        .saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | righe_scritte={rows} | target={TARGET_TABLE}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
