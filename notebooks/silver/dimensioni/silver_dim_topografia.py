# Databricks notebook source
# Area: Dimensioni
# Layer: Silver
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: Dimensione Topografia (celle di magazzino) — da bronze.logistica.struttura_mag.
#              Colonne reali verificate sul Bronze (SOURCE_COLS struttura_mag):
#                MAG_SITO_COD, STRM_COD_MAGAZZINO, STRM_CORSIA, STRM_COLONNA, STRM_PIANO,
#                STRM_COD_CLAS_POSPA, STRM_COD_ZONA_MAG, STRM_COD_SETTOR_MAG, STRM_STATO_POSPA.
#              CELLA_COD = concat_ws('_', MAG_SITO_COD, STRM_COD_MAGAZZINO, STRM_CORSIA,
#              STRM_COLONNA, STRM_PIANO). Anagrafica FULL -> overwrite completo (stato corrente).

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog, normalize_sito, get_sito_alias_map

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date

# COMMAND ----------
# MAGIC %md #### 1. Widget standard

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------
# MAGIC %md #### 2. Parametri notebook

# COMMAND ----------

NOTEBOOK_NAME  = "silver_dim_topografia"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{BRONZE_CATALOG}.{SCHEMA}.struttura_mag"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.dim_topografia"
MERGE_KEY      = "CELLA_COD"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------
# MAGIC %md #### 3. Lettura, mapping colonne reali STRM_ e dedup

# COMMAND ----------

try:
    _amap = get_sito_alias_map(spark, f"{BRONZE_CATALOG}.{SCHEMA}")
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    # Anagrafica logistica FULL: leggi tutto (nessun filtro su _bronze_load_date)
    raw_df = spark.table(SOURCE_TABLE)
    rows_read = raw_df.count()
    logger.info(f"Righe lette da {SOURCE_TABLE}: {rows_read}")

    check_not_null(raw_df, ["MAG_SITO_COD", "STRM_COD_MAGAZZINO"], NOTEBOOK_NAME)

    mapped_df = (
        raw_df
        .withColumn(
            "CELLA_COD",
            F.concat_ws(
                "_",
                F.col("MAG_SITO_COD"), F.col("STRM_COD_MAGAZZINO"),
                F.col("STRM_CORSIA"), F.col("STRM_COLONNA"), F.col("STRM_PIANO"),
            ),
        )
        .select(
            F.col("CELLA_COD").cast("string").alias("CELLA_COD"),
            normalize_sito(F.col("MAG_SITO_COD"), _amap).alias("SITO_COD"),
            F.col("STRM_COD_MAGAZZINO").cast("string").alias("MAG_COD"),
            F.col("STRM_CORSIA").cast("string").alias("CORSIA"),
            F.col("STRM_COLONNA").cast("string").alias("COLONNA"),
            F.col("STRM_PIANO").cast("string").alias("PIANO"),
            F.col("STRM_COD_CLAS_POSPA").cast("string").alias("COD_CLAS_POSPA"),
            F.col("STRM_COD_ZONA_MAG").cast("string").alias("COD_ZONA_MAG"),
            F.col("STRM_COD_SETTOR_MAG").cast("string").alias("COD_SETTOR_MAG"),
            F.col("STRM_STATO_POSPA").cast("string").alias("STATO_POSPA"),
            F.col("_bronze_insert_ts"),
        )
    )

    # Dedup per CELLA_COD: ultima versione per _bronze_insert_ts
    w = Window.partitionBy("CELLA_COD").orderBy(F.col("_bronze_insert_ts").desc())

    silver_df = (
        mapped_df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_bronze_insert_ts")
        .withColumn("_silver_ts", F.current_timestamp())
        .select(
            "CELLA_COD", "SITO_COD", "MAG_COD", "CORSIA", "COLONNA", "PIANO",
            "COD_CLAS_POSPA", "COD_ZONA_MAG", "COD_SETTOR_MAG", "STATO_POSPA",
            "_silver_ts",
        )
    )

    rows_clean = silver_df.count()
    logger.info(f"Righe dopo dedup: {rows_clean}")
    check_row_count(silver_df, min_rows=1, notebook_name=NOTEBOOK_NAME)



    (silver_df.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | righe_lette={rows_read} | righe_silver={rows_clean}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
