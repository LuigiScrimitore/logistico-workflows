# Databricks notebook source
# Area: Giacenze / Stock (migrazione TO-BE)
# Layer: Silver (cleansing)
# Versione: 1.0.0
# Data: 2026-06-09
# Descrizione: Cleansing della sorgente bronze.logistica.catena (RAW logistix, prefisso CATE_).
#              SOLO cleansing via utility condivise (1 riga in -> 1 riga out, no business logic):
#                - date Julian -> DateType (julian_to_date)
#                - normalize_sito su MAG_SITO_COD (alias map da TABGEN, get_sito_alias_map)
#                - art_radice/art_variante per derivare ART_RADICE_COD/ART_VAR_LOGIS_COD da CATE_COD_MSI
#                - trim/cast espliciti
#              Sostituisce la derivazione anticipata fn_get_radice nello staging WL1 (anomalia §4.6).
#              Filtro snapshot: _bronze_load_date = run_date. Nessun join, nessun dedup di business.
#              Riferimento: Linee guida §3 (cleansing), §11 (riga WL1_CATENA: mag_sito + ART_RADICE/VAR).

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog, julian_to_date, normalize_sito, get_sito_alias_map, art_radice, art_variante

from pyspark.sql import functions as F
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME  = "silver_catena_clean"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{BRONZE_CATALOG}.{SCHEMA}.catena"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.catena_clean"

# Colonne data (Julian Day) da convertire
JULIAN_COLS = ["CATE_DATA_CARICO", "CATE_DATA_SCADENZA", "ETL_DATINS"]

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    if not spark.catalog.tableExists(SOURCE_TABLE):
        logger.warning(f"Sorgente {SOURCE_TABLE} non esiste. Notebook terminato.")
        dbutils.notebook.exit("NO_SOURCE")

    raw_df = spark.table(SOURCE_TABLE).filter(F.col("_bronze_load_date") == F.lit(run_date))
    rows_read = raw_df.count()
    logger.info(f"Righe lette da {SOURCE_TABLE} per _bronze_load_date={run_date}: {rows_read}")
    if rows_read == 0:
        logger.warning("Nessun dato. Notebook terminato.")
        dbutils.notebook.exit("NO_DATA")

    _amap = get_sito_alias_map(spark, f"{BRONZE_CATALOG}.{SCHEMA}")

    # ── Cleansing: 1 riga in -> 1 riga out ────────────────────────────────────
    silver_df = raw_df

    # FIX (sessione test): CATENA raw NON ha MAG_SITO_COD (nel legacy e' iniettato via
    # lookup TABGEN nro_tab=7). Lo deriviamo dal _sito_estrazione (db-link LGAX/LGCX)
    # normalizzato al canonico numerico. Coerente con decisione B (sito derivato in Silver).
    silver_df = silver_df.withColumn("MAG_SITO_COD", normalize_sito(F.col("_sito_estrazione"), _amap))

    # derivazione radice/variante da CATE_COD_MSI (sorgente LOGISTIX -> troncamento)
    silver_df = (
        silver_df
        .withColumn("ART_RADICE_COD", art_radice(F.trim(F.col("CATE_COD_MSI"))))
        .withColumn("ART_VAR_LOGIS_COD", art_variante(F.trim(F.col("CATE_COD_MSI"))))
    )

    # date Julian -> DateType (solo colonne presenti)
    for c in JULIAN_COLS:
        if c in silver_df.columns:
            silver_df = silver_df.withColumn(c, julian_to_date(F.col(c)))

    silver_df = silver_df.withColumn("_silver_ts", F.current_timestamp())

    check_not_null(silver_df, ["MAG_SITO_COD", "CATE_COD_MSI", "CATE_NRO_ETICHETTA"], NOTEBOOK_NAME)
    rows_clean = silver_df.count()
    logger.info(f"Righe silver (cleansing 1:1): {rows_clean}")
    check_row_count(silver_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    # SNAPSHOT: append partizionato per data snapshot bronze
    # Idempotente: dynamic partition overwrite -> ri-eseguire lo stesso run_date
    # sovrascrive SOLO quella partizione-giorno (no raddoppio), mantiene lo storico.
    (silver_df.write.format("delta").mode("overwrite").option("partitionOverwriteMode", "dynamic")
     .partitionBy("_bronze_load_date").option("mergeSchema", "true")
     .saveAsTable(TARGET_TABLE))
    logger.info(f"SNAPSHOT append {TARGET_TABLE} ({rows_clean} righe per run_date={run_date})")

    logger.info(f"END {NOTEBOOK_NAME} | righe_lette={rows_read} | righe_silver={rows_clean}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
