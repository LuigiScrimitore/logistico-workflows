# Databricks notebook source
# Area: Tracciabilità - Carrellisti
# Layer: Silver
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: Sessione Carrellista Silver — lettura da bronze.logistica.cartellino (CARTE_).
#              Allineato alle COLONNE REALI CARTE_ del Bronze (bronze_cartellino.py):
#              MAG_SITO_COD, CARTE_COD_CARRELLIST, CARTE_DATA, CARTE_LOGIN, CARTE_LOGOUT,
#              CARTE_ATTUALE.
#              Calcolo ORE_PRESENZA da CARTE_LOGIN/CARTE_LOGOUT.
#              Chiave naturale: MAG_SITO_COD + CARTE_COD_CARRELLIST + CARTE_DATA.
#              MERGE INTO silver.logistica.sessione_carrellista (CTAS prima volta).
#
# NOTA REVISIONE 3.0.0: rimosse colonne inventate non presenti nel Bronze reale:
#   CARTE_COD_CARRELLISTA (reale: CARTE_COD_CARRELLIST), CARTE_ORA_ENTRATA,
#   CARTE_ORA_USCITA, CARTE_MINUTI_PAUSA, CARTE_MINUTI_RIUNIONE. Di conseguenza
#   ORE_PRODUTTIVE (che dipendeva da pause/riunioni inesistenti) è RIMOSSA: il Bronze
#   non espone i tempi non produttivi. Le entrate/uscite reali sono CARTE_LOGIN/LOGOUT.
#   CARTE_ATTUALE = flag sessione attualmente aperta.

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog, julian_to_date, normalize_sito, get_sito_alias_map

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME  = "silver_sessione_carrellista"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{BRONZE_CATALOG}.{SCHEMA}.cartellino"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.sessione_carrellista"

# COMMAND ----------

logger = get_logger(NOTEBOOK_NAME)

try:
    _amap = get_sito_alias_map(spark, f"{BRONZE_CATALOG}.{SCHEMA}")
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    raw_df = (
        spark.table(SOURCE_TABLE)
        .filter(F.col("_bronze_load_date") == run_date)
    )
    rows_read = raw_df.count()
    logger.info(f"Righe lette da {SOURCE_TABLE} per run_date={run_date}: {rows_read}")

    check_not_null(raw_df, ["MAG_SITO_COD", "CARTE_COD_CARRELLIST", "CARTE_DATA"], NOTEBOOK_NAME)

    # Deduplica su chiave naturale
    w = Window.partitionBy("MAG_SITO_COD", "CARTE_COD_CARRELLIST", "CARTE_DATA") \
              .orderBy(F.col("_bronze_insert_ts").desc())

    dedup_df = (
        raw_df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("SITO_COD",        normalize_sito(F.col("MAG_SITO_COD"), _amap))
        .withColumn("CARRELLISTA_COD", F.col("CARTE_COD_CARRELLIST").cast("string"))
        .withColumn("DATA_PRESENZA",   julian_to_date(F.col("CARTE_DATA")))
        # CARTE_LOGIN/CARTE_LOGOUT = entrata/uscita reali (timestamp). try_cast: valori sentinella
        # sporchi (es. '0') -> NULL invece di crash ANSI (CAST_INVALID_INPUT). ACT_9027.
        .withColumn("ORA_LOGIN",       F.expr("try_cast(CARTE_LOGIN as timestamp)"))
        .withColumn("ORA_LOGOUT",      F.expr("try_cast(CARTE_LOGOUT as timestamp)"))
        # CARTE_ATTUALE: 'S' = sessione attualmente aperta
        .withColumn("FLG_SESSIONE_APERTA", (F.col("CARTE_ATTUALE") == F.lit("S")))
    )

    # ORE_PRESENZA = (CARTE_LOGOUT - CARTE_LOGIN) / 3600
    silver_df = (
        dedup_df
        .withColumn(
            "ORE_PRESENZA",
            F.when(
                F.col("ORA_LOGOUT").isNotNull() & F.col("ORA_LOGIN").isNotNull(),
                # try_cast via expr (F.try_cast non esiste in questa versione): durate anomale
                # fuori range Decimal(6,2) -> NULL invece di crash ANSI (NUMERIC_VALUE_OUT_OF_RANGE). ACT_9027.
                F.expr("try_cast((unix_timestamp(ORA_LOGOUT) - unix_timestamp(ORA_LOGIN)) / 3600.0 as decimal(6,2))")
            ).otherwise(F.lit(None).cast("decimal(6,2)"))
        )
        .select(
            "SITO_COD", "CARRELLISTA_COD", "DATA_PRESENZA",
            "ORA_LOGIN", "ORA_LOGOUT", "FLG_SESSIONE_APERTA", "ORE_PRESENZA",
            "_bronze_insert_ts", "_bronze_load_date", "_sito_cod",
        )
        .withColumn("_silver_ts", F.current_timestamp())
    )

    rows_clean = silver_df.count()
    incomplete_cnt = silver_df.filter(F.col("ORA_LOGOUT").isNull()).count()
    logger.info(f"Righe silver dopo deduplica: {rows_clean} | senza LOGOUT: {incomplete_cnt}")
    if incomplete_cnt > 0:
        logger.warning(f"Sessioni incomplete (CARTE_LOGOUT nullo / sessione aperta): {incomplete_cnt}")

    check_row_count(silver_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    if not spark.catalog.tableExists(TARGET_TABLE):
        logger.info(f"Prima esecuzione — CTAS {TARGET_TABLE}")
        silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)
    else:
        DeltaTable.forName(spark, TARGET_TABLE).alias("tgt").merge(
            silver_df.alias("src"),
            "tgt.SITO_COD = src.SITO_COD "
            "AND tgt.CARRELLISTA_COD = src.CARRELLISTA_COD "
            "AND tgt.DATA_PRESENZA = src.DATA_PRESENZA"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    logger.info(
        f"END {NOTEBOOK_NAME} | righe_lette={rows_read} | righe_silver={rows_clean} "
        f"| incomplete={incomplete_cnt}"
    )

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
