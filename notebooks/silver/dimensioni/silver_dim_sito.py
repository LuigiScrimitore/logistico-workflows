# Databricks notebook source
# Area: Dimensioni
# Layer: Silver
# Versione: 4.0.0
# Autore: Luigi Scrimitore / Francesco Foconi
# Data: 2026-09-03
# Descrizione: Dimensione Sito logistico. Costruita da S_LOGISTIX (anagrafica autoritativa dei
#              22 siti attivi: DBLINK_NAME alfa, DBLINK_DESC nome sito, MAG_SITO_COD canonico
#              alfa 0020A) join WL1_MAG_SITO_STORICO (fornisce il codice NUMERICO usato dalle
#              transazionali: MAG_SITO_COD_ORIG). Filtro correnti+attivi su WL1:
#              DATFIN_VALID=99999999 AND MAG_SITO_ORIG_ATTIVO=1 (mapping univoco, verificato).
#              SITO_COD canonico = numerico 2 cifre zero-padded (== output normalize_sito usato
#              da silver_ordini/trasporti/spedizioni): elimina gli orphan sito dei trasporti.
#              v3.0.0 (deprecata): SITO_COD da struttura_mag (solo 5 siti, SITO_DESC null) -> ACT_9026.
#              Anagrafica logistica FULL -> overwrite completo (stato corrente).

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog

from pyspark.sql import functions as F
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

NOTEBOOK_NAME  = "silver_dim_sito"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SRC_SLOGISTIX  = f"{BRONZE_CATALOG}.{SCHEMA}.s_logistix"
SRC_WL1        = f"{BRONZE_CATALOG}.{SCHEMA}.wl1_mag_sito_storico"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.dim_sito"
MERGE_KEY      = "SITO_COD"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------
# MAGIC %md #### 3. Lettura sorgenti (S_LOGISTIX = autorita' siti; WL1 = codice numerico)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    for src in (SRC_SLOGISTIX, SRC_WL1):
        if not spark.catalog.tableExists(src):
            logger.error(f"Sorgente mancante: {src}. Esegui prima il bronze (landing_ingestion).")
            dbutils.notebook.exit("NO_SOURCE")

    slog = spark.table(SRC_SLOGISTIX)
    wl1  = spark.table(SRC_WL1)
    logger.info(f"Righe S_LOGISTIX={slog.count()} | WL1={wl1.count()}")

    check_not_null(slog, ["MAG_SITO_COD"], NOTEBOOK_NAME)

    # ── WL1 correnti+attivi: MAG_SITO_COD (alfa) -> codice NUMERICO (2 cifre) ──────
    #   Robusto a bronze string/parquet: cast double per i confronti numerici e per
    #   normalizzare "20"/"20.0"; poi solo cifre + lpad(2) = formato normalize_sito.
    wl1_active = (
        wl1
        .filter((F.col("DATFIN_VALID").cast("double") == F.lit(99999999)) &
                (F.col("MAG_SITO_ORIG_ATTIVO").cast("double") == F.lit(1)))
        .withColumn("_mscod", F.upper(F.trim(F.col("MAG_SITO_COD"))))
        .withColumn("_num", F.lpad(
            F.regexp_replace(F.col("MAG_SITO_COD_ORIG").cast("double").cast("long").cast("string"),
                             r"[^0-9]", ""), 2, "0"))
        .filter(F.col("_num").isNotNull() & (F.col("_num") != ""))
        .select("_mscod", "_num")
        .dropDuplicates(["_mscod", "_num"])
    )

    slog_n = (
        slog
        .withColumn("_mscod", F.upper(F.trim(F.col("MAG_SITO_COD"))))
        .withColumn("SITO_DESC", F.trim(F.col("DBLINK_DESC")))
        .withColumn("SITO_COD_ALFA", F.upper(F.regexp_replace(F.trim(F.col("DBLINK_NAME")), r"^LOG_", "")))
    )

    # ── Join autorita' (S_LOGISTIX) x codice numerico (WL1) ───────────────────────
    dim = (
        slog_n.join(wl1_active, on="_mscod", how="inner")
        .select(
            F.col("_num").alias("SITO_COD"),          # canonico numerico 2 cifre (join transazionali)
            F.col("SITO_DESC"),
            F.col("SITO_COD_ALFA"),                    # 4-char alfa (LGAX) da DBLINK_NAME — riferimento
            F.col("_mscod").alias("SITO_COD_MAG"),     # canonico alfa 0020A (S_LOGISTIX.MAG_SITO_COD)
        )
        .dropDuplicates(["SITO_COD"])
        .withColumn("_silver_ts", F.current_timestamp())
    )

    rows_clean = dim.count()
    n_slog = slog.count()
    logger.info(f"Siti dimensione (con codice numerico): {rows_clean} / S_LOGISTIX {n_slog}")
    if rows_clean < n_slog:
        logger.warning(f"{n_slog - rows_clean} siti S_LOGISTIX senza codice numerico attivo in WL1 "
                       f"(non referenziabili dalle transazionali numeriche) — esclusi dalla dim.")
    check_row_count(dim, min_rows=1, notebook_name=NOTEBOOK_NAME)

    (dim.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | righe_silver={rows_clean}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
