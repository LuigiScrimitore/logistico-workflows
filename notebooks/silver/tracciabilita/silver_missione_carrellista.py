# Databricks notebook source
# Area: Tracciabilità - Carrellisti
# Layer: Silver
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: Missioni Carrellista Silver — lettura da bronze.logistica.dettaglio_carr (DTCRL_).
#              Allineato alle COLONNE REALI DTCRL_ del Bronze (bronze_missioni_carr.py).
#              Chiave naturale: MAG_SITO_COD + DTCRL_COD_CARRELLIST + DTCRL_DATA_RICH_ABB
#              + DTCRL_ORA_RICH_ABB + DTCRL_COD_MSI.
#              MERGE INTO silver.logistica.missione_carrellista (CTAS prima volta).
#
# NOTA REVISIONE 3.0.0: rimosse colonne inventate non presenti nel Bronze reale:
#   DTCRL_COD_CARRELLISTA (reale: DTCRL_COD_CARRELLIST), DTCRL_DATA_MISSIONE,
#   DTCRL_NRO_MISSIONE, DTCRL_TIPO, DTCRL_INIZIO, DTCRL_FINE, DTCRL_DURATA,
#   DTCRL_QTA_MOVIMENTATA, DTCRL_COD_ARTICOLO, DTCRL_COD_MAGAZZINO.
#   La missione è identificata da DTCRL_COD_MSI. Le finestre temporali reali sono
#   richiesta/inizio/effettivo abbinamento (DATA/ORA_RICH/INIZ/EFFET_ABB) e gli
#   effettivi DTCRL_DATA_EFFETTIVA_INIZIO/FINE. DURATA_MIN ricavata da questi ultimi.

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

NOTEBOOK_NAME  = "silver_missione_carrellista"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{BRONZE_CATALOG}.{SCHEMA}.dettaglio_carr"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.missione_carrellista"

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

    check_not_null(
        raw_df,
        ["MAG_SITO_COD", "DTCRL_COD_CARRELLIST", "DTCRL_DATA_RICH_ABB",
         "DTCRL_ORA_RICH_ABB", "DTCRL_COD_MSI"],
        NOTEBOOK_NAME
    )

    # Deduplica su chiave naturale reale
    w = Window.partitionBy(
        "MAG_SITO_COD", "DTCRL_COD_CARRELLIST",
        "DTCRL_DATA_RICH_ABB", "DTCRL_ORA_RICH_ABB", "DTCRL_COD_MSI"
    ).orderBy(F.col("_bronze_insert_ts").desc())

    silver_df = (
        raw_df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            normalize_sito(F.col("MAG_SITO_COD"), _amap).alias("SITO_COD"),
            F.col("DTCRL_COD_CARRELLIST").cast("string").alias("CARRELLISTA_COD"),
            F.col("DTCRL_COD_MSI").cast("string").alias("MISSIONE_COD"),
            julian_to_date(F.col("DTCRL_DATA_RICH_ABB")).alias("DATA_RICH_ABB"),
            F.col("DTCRL_ORA_RICH_ABB").cast("string").alias("ORA_RICH_ABB"),
            julian_to_date(F.col("DTCRL_DATA_INIZ_ABB")).alias("DATA_INIZ_ABB"),
            F.col("DTCRL_ORA_INIZ_ABB").cast("string").alias("ORA_INIZ_ABB"),
            julian_to_date(F.col("DTCRL_DATA_EFFET_ABB")).alias("DATA_EFFET_ABB"),
            F.col("DTCRL_ORA_EFFET_ABB").cast("string").alias("ORA_EFFET_ABB"),
            F.col("DTCRL_DATA_EFFETTIVA_INIZIO").cast("timestamp").alias("DATA_EFFETTIVA_INIZIO"),
            F.col("DTCRL_DATA_EFFETTIVA_FINE").cast("timestamp").alias("DATA_EFFETTIVA_FINE"),
            F.col("DTCRL_CORSIA_PARTENZ").cast("string").alias("CORSIA_PARTENZA"),
            F.col("DTCRL_COLONNA_PARTEN").cast("string").alias("COLONNA_PARTENZA"),
            F.col("DTCRL_PIANO_PARTENZA").cast("string").alias("PIANO_PARTENZA"),
            F.col("DTCRL_LIVELLO_PARTENZA").cast("string").alias("LIVELLO_PARTENZA"),
            F.col("DTCRL_CORSIA_ARRIVO").cast("string").alias("CORSIA_ARRIVO"),
            F.col("DTCRL_COLONNA_ARRIVO").cast("string").alias("COLONNA_ARRIVO"),
            F.col("DTCRL_PIANO_ARRIVO").cast("string").alias("PIANO_ARRIVO"),
            F.col("DTCRL_LIVELLO_ARRIVO").cast("string").alias("LIVELLO_ARRIVO"),
            F.col("DTCRL_COD_SETTOR_MAG").cast("string").alias("SETTORE_MAG_COD"),
            F.col("DTCRL_COD_ENTE_RICH").cast("string").alias("ENTE_RICH_COD"),
            F.col("DTCRL_NRO_ETICHETTA").cast("string").alias("ETICHETTA_NRO"),
            F.col("DTCRL_NRO_CARICO").cast("string").alias("CARICO_NRO"),
            F.col("DTCRL_NRO_ORDINE").cast("string").alias("ORDINE_NRO"),
            F.col("DTCRL_COD_PORTA").cast("string").alias("PORTA_COD"),
            F.col("DTCRL_NRO_GABBIA").cast("string").alias("GABBIA_NRO"),
            F.col("DTCRL_COD_NEGOZIO").cast("string").alias("NEGOZIO_COD"),
            F.col("DTCRL_NRO_RIEPILOGO").cast("string").alias("RIEPILOGO_NRO"),
            F.col("DTCRL_DOPPIO_MOVIM").cast("string").alias("DOPPIO_MOVIM"),
            F.col("DTCRL_NRO_PICKING").cast("string").alias("PICKING_NRO"),
            F.col("_bronze_insert_ts"),
            F.col("_bronze_load_date"),
            F.col("_sito_cod"),
        )
        # DURATA_MIN: dalle date/ora effettive inizio/fine (uniche temporali reali utilizzabili).
        .withColumn(
            "DURATA_MIN",
            F.when(
                F.col("DATA_EFFETTIVA_INIZIO").isNotNull() & F.col("DATA_EFFETTIVA_FINE").isNotNull(),
                ((F.unix_timestamp(F.col("DATA_EFFETTIVA_FINE"))
                  - F.unix_timestamp(F.col("DATA_EFFETTIVA_INIZIO"))) / F.lit(60.0)).cast("decimal(10,2)")
            ).otherwise(F.lit(None).cast("decimal(10,2)"))
        )
        .withColumn("_silver_ts", F.current_timestamp())
    )

    rows_clean = silver_df.count()
    logger.info(f"Righe silver dopo deduplica: {rows_clean}")
    check_row_count(silver_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    if not spark.catalog.tableExists(TARGET_TABLE):
        logger.info(f"Prima esecuzione — CTAS {TARGET_TABLE}")
        silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)
    else:
        DeltaTable.forName(spark, TARGET_TABLE).alias("tgt").merge(
            silver_df.alias("src"),
            "tgt.SITO_COD = src.SITO_COD "
            "AND tgt.CARRELLISTA_COD = src.CARRELLISTA_COD "
            "AND tgt.DATA_RICH_ABB = src.DATA_RICH_ABB "
            "AND tgt.ORA_RICH_ABB = src.ORA_RICH_ABB "
            "AND tgt.MISSIONE_COD = src.MISSIONE_COD"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    logger.info(f"END {NOTEBOOK_NAME} | righe_lette={rows_read} | righe_silver={rows_clean}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
