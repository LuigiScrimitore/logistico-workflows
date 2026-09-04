# Databricks notebook source
# Area: Preparazione Spedizioni — Gold (Fact produttivita'/turno)
# Versione: 1.0.0  Data: 2026-06-10
# Tabella: F_TURNO_PREP_SITO  (grana = riepilogo per SITO, DATA_PREPARAZ, PREPARATORE, RIEPILOGO_NRO)
#
# STANDARD 2-NOTEBOOK (Linee guida §1-bis):
#   FASE 3 (normalizzazione) — questo notebook fa SOLO aggancio dimensioni + scrittura.
#   La produttivita' (regola 30 min, ORE_PRODUTTIVE, PRODUTTIVITA_CARTONI_ORA) e' gia'
#   calcolata in silver.logistica_curated.turno_prep_sito.
#
# 🔒 REGOLA D'ORO: legge SOLO da silver.logistica_curated.turno_prep_sito.
# Storia: rimpiazza lo stub DEPRECATED_OP06; assorbe la logica produttivita' che prima
#         viveva impropriamente in gold_f_prep_sped v3. Sorgente naturale degli aggregati
#         A_TURNO_PREP_SITO (gold_dm_turno_prep_sito) e A_PRODUTTIVITA_MENSILE.

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from utils import get_catalog, surrogate_key_fallback
from dq_helper import check_orphan_rate
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, DateType, BooleanType, IntegerType
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

NOTEBOOK_NAME  = "gold_f_turno_prep_sito"
SILVER_CATALOG = get_catalog("silver", env)
GOLD_CATALOG   = get_catalog("gold",   env)
SOURCE_TABLE   = f"{SILVER_CATALOG}.logistica_curated.turno_prep_sito"   # 🔒 unica sorgente
TARGET_TABLE   = f"{GOLD_CATALOG}.logistica.F_TURNO_PREP_SITO"

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
        dbutils.notebook.exit("NO_DATA")

    fact = src.select(
        F.col("SITO_COD").cast(StringType()).alias("SITO_COD"),
        F.col("DATA_PREPARAZ").cast(DateType()).alias("DATA_PREPARAZ"),
        F.col("PREPARATORE_COD").cast(StringType()).alias("PREPARATORE_COD"),
        F.col("RIEPILOGO_NRO").cast(StringType()).alias("RIEPILOGO_NRO"),
        F.col("NEGOZIO_COD").cast(StringType()).alias("PDV_COD"),
        F.col("NEGOZIO_COD").cast(StringType()).alias("PDV_COD_NAT"),
        F.col("AREA_MERCEOLOGICA_COD").cast(StringType()).alias("AREA_MERCEOLOGICA_COD"),
        F.col("ZONA_MAG_COD").cast(StringType()).alias("ZONA_MAG_COD"),
        F.col("REPARTO_PREP_COD").cast(StringType()).alias("REPARTO_PREP_COD"),
        F.col("TIPO_IMBALLO").cast(StringType()).alias("TIPO_IMBALLO"),
        F.col("TIPO_RIEPILOGO").cast(StringType()).alias("TIPO_RIEPILOGO"),
        F.col("TS_INIZIO").alias("TS_INIZIO"),
        F.col("TS_FINE").alias("TS_FINE"),
        F.col("ORE_LAVORATE").cast(DoubleType()).alias("ORE_LAVORATE"),
        F.col("ORE_PRODUTTIVE").cast(DoubleType()).alias("ORE_PRODUTTIVE"),
        F.col("FLAG_TEMPO_ASSENTE").cast(BooleanType()).alias("FLAG_TEMPO_ASSENTE"),
        F.col("TOT_CARTONI").cast(DoubleType()).alias("TOT_CARTONI"),
        F.col("TOT_CARTONI_PREP").cast(DoubleType()).alias("TOT_CARTONI_PREP"),
        F.col("TOT_CARTONI_INEVASI").cast(DoubleType()).alias("TOT_CARTONI_INEVASI"),
        F.col("TOT_QUINTALI").cast(DoubleType()).alias("TOT_QUINTALI"),
        F.col("TOT_QUINTALI_PREP").cast(DoubleType()).alias("TOT_QUINTALI_PREP"),
        F.col("TOT_QUINTALI_INEVASI").cast(DoubleType()).alias("TOT_QUINTALI_INEVASI"),
        F.col("NUM_PREPARATI").cast(IntegerType()).alias("NUM_PREPARATI"),
        F.col("NUM_INEVASI").cast(IntegerType()).alias("NUM_INEVASI"),
        F.col("NUM_REFERENZE").cast(IntegerType()).alias("NUM_REFERENZE"),
        F.col("GABBIE_PREPARATE").cast(IntegerType()).alias("GABBIE_PREPARATE"),
        F.col("PRODUTTIVITA_CARTONI_ORA").cast(DoubleType()).alias("PRODUTTIVITA_CARTONI_ORA"),
        F.col("FLAG_ESEGUITO").cast(StringType()).alias("FLAG_ESEGUITO"),
        F.current_timestamp().alias("DWH_UPDATED_AT"),
    )

    # Chiavi naturali pre-risoluzione surrogate — necessarie per LAD resolver (OP-32).
    # PDV_COD_NAT già inserita nel select() sopra (= NEGOZIO_COD originale).
    fact = (fact
            .withColumn("SITO_COD_NAT",              F.col("SITO_COD"))
            .withColumn("PREPARATORE_COD_NAT",       F.col("PREPARATORE_COD"))
            .withColumn("AREA_MERCEOLOGICA_COD_NAT", F.col("AREA_MERCEOLOGICA_COD")))

    # ── FASE 3: aggancio dimensioni ─────────────────────────────────────────────
    try:
        lu_pdv = spark.read.table(f"{retail_ms}.LU_PDV")
        fact = surrogate_key_fallback(fact, "PDV_COD", lu_pdv, "PDV_COD", default_val="-1")
        check_orphan_rate(fact, "PDV_COD", NOTEBOOK_NAME)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"LU_PDV non disponibile in {retail_ms}: aggancio PDV saltato ({str(e)[:80]})")

    LOGISTIC_LU = [
        ("SITO_COD",              "LU_SITO",             "SITO_COD"),
        ("PREPARATORE_COD",       "LU_OPERATORE",        "OPERATORE_COD"),
        ("AREA_MERCEOLOGICA_COD", "LU_AREA_MERCL_LOGIS", "COD_AREA_MERC"),
    ]
    for fk, lu_tab, lu_pk in LOGISTIC_LU:
        try:
            lu_df = spark.read.table(f"{GOLD_CATALOG}.logistica.{lu_tab}")
            # preparatore: NULL in origine -> membro 'ND' (Non rilevato), non orfano.
            _nv = "ND" if fk == "PREPARATORE_COD" else None
            fact = surrogate_key_fallback(fact, fk, lu_df, lu_pk, default_val="-1", null_val=_nv)
            check_orphan_rate(fact, fk, NOTEBOOK_NAME)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"{lu_tab} non disponibile: aggancio {fk} saltato ({str(e)[:80]})")

    rows = fact.count()
    logger.info(f"F_TURNO_PREP_SITO righe: {rows}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.logistica")
    (fact.write.format("delta").mode("overwrite").option("partitionOverwriteMode", "dynamic")
        .option("mergeSchema", "true")
        .partitionBy("DATA_PREPARAZ")
        .saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | righe_scritte={rows} | target={TARGET_TABLE}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
