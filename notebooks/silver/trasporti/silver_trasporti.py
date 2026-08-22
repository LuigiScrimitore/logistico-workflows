# Databricks notebook source
# ⚠️ DEPRECATO (2026-06-10, standard 2-notebook §1-bis + scelta B).
# Leggeva bronze.logistica.t_trasp_mtv (STAGING bronze) → silver.logistica.trasporto,
# consumato da gold_f_trasporto v3. Incoerente con scelta B (no staging).
# Sostituito dalla catena rebuilt-from-raw: silver_trasp_mtv_build → s_trasp_mtv
#   → silver_prep_trasporto → silver.logistica_curated.trasporto → gold_f_trasporto v4.
# NON eseguire.
import sys as _sys
try:
    dbutils.notebook.exit("DEPRECATED_USE_silver_prep_trasporto")  # noqa: F821
except Exception:
    _sys.exit(0)

# Area: Trasporti (NON CORE — flag header, da rivalutare in scoping)
# Layer: Silver
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: Trasporti Silver — lettura da bronze.logistica.t_trasp_mtv (sistema CND).
#              Allineato alle COLONNE REALI SP_*/MTV del Bronze (bronze_trasporti.py):
#              SP_ID, AUTISTA_SPED_COD, AUTOMEZZO, VETTORE, DATABOLLA, NUMBOLLA, STATO,
#              DATACONSPREV, AZIONE_COD, DATA_AZIONE, MTV_COD, QTA, SP_NUMSIGILLORIT,
#              MAG_SITO_COD, SOCIO_COD, MAG_SITO_TRANSITO_COD, NUM_GITA.
#              Chiave naturale: SP_ID + MAG_SITO_COD + DATABOLLA + NUMBOLLA.
#              MERGE INTO silver.logistica.trasporto (CTAS prima volta).
#
# NOTA REVISIONE 3.0.0: le colonne SP_CODVET, SP_PESOKG, SP_VOLUME, SP_NUMCOLL,
#   SP_DATACONSEGNAPREV, SP_DATACONSEGNAEFF, SP_FLAGRITARDO, SP_NOTE NON ESISTONO nel
#   Bronze reale ed erano inventate: rimosse. Il corriere/vettore reale è VETTORE,
#   la data consegna prevista è DATACONSPREV. Non esiste una data consegna effettiva
#   né un flag ritardo nativo: LEAD_TIME_GG e FLG_RITARDO vengono ricavati dalla
#   DATA_AZIONE (ultima azione) quando l'azione rappresenta la consegna; in assenza
#   di mappatura azioni certa restano informativi e potranno essere rivisti in Gold.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog, normalize_sito, get_sito_alias_map

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

NOTEBOOK_NAME  = "silver_trasporti"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{BRONZE_CATALOG}.{SCHEMA}.t_trasp_mtv"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.trasporto"

# COMMAND ----------

logger = get_logger(NOTEBOOK_NAME)

try:
    _amap = get_sito_alias_map(spark, f"{BRONZE_CATALOG}.{SCHEMA}")
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    # Transazionale: filtra il Bronze delta giornaliero per run_date
    raw_df = (
        spark.table(SOURCE_TABLE)
        .filter(F.col("_bronze_load_date") == run_date)
    )
    rows_read = raw_df.count()
    logger.info(f"Righe lette da {SOURCE_TABLE} per run_date={run_date}: {rows_read}")

    check_not_null(raw_df, ["SP_ID", "MAG_SITO_COD", "DATABOLLA", "NUMBOLLA"], NOTEBOOK_NAME)

    # Deduplica su chiave naturale, tenendo l'ultima versione per _bronze_insert_ts
    w = Window.partitionBy("SP_ID", "MAG_SITO_COD", "DATABOLLA", "NUMBOLLA") \
              .orderBy(F.col("_bronze_insert_ts").desc())

    silver_df = (
        raw_df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            F.col("SP_ID").cast("string").alias("TRASPORTO_ID"),
            normalize_sito(F.col("MAG_SITO_COD"), _amap).alias("SITO_COD"),
            F.col("DATABOLLA").cast("date").alias("DATA_BOLLA"),
            F.col("NUMBOLLA").cast("string").alias("BOLLA_NRO"),
            # VETTORE è il codice/identificativo vettore reale (NON SP_CODVET inesistente)
            F.col("VETTORE").cast("string").alias("CORRIERE_COD"),
            F.col("AUTISTA_SPED_COD").cast("string").alias("AUTISTA_SPED_COD"),
            F.col("AUTOMEZZO").cast("string").alias("AUTOMEZZO"),
            F.col("STATO").cast("string").alias("STATO"),
            F.col("MTV_COD").cast("string").alias("MOTIVO_COD"),
            F.col("AZIONE_COD").cast("string").alias("AZIONE_COD"),
            F.col("DATA_AZIONE").cast("date").alias("DATA_AZIONE"),
            F.col("QTA").cast("decimal(14,3)").alias("QTA"),
            F.col("DATACONSPREV").cast("date").alias("DATA_CONSEGNA_PREV"),
            F.col("SP_NUMSIGILLORIT").cast("string").alias("NUM_SIGILLO_RIT"),
            F.col("SOCIO_COD").cast("string").alias("SOCIO_COD"),
            F.col("MAG_SITO_TRANSITO_COD").cast("string").alias("SITO_TRANSITO_COD"),
            F.col("NUM_GITA").cast("string").alias("NUM_GITA"),
            F.col("_bronze_insert_ts"),
            F.col("_bronze_load_date"),
        )
        # LEAD_TIME_GG: differenza tra la data dell'ultima azione (proxy consegna) e
        # la data bolla. Non esiste una data consegna effettiva dedicata nel Bronze.
        .withColumn(
            "LEAD_TIME_GG",
            F.when(
                F.col("DATA_AZIONE").isNotNull() & F.col("DATA_BOLLA").isNotNull(),
                F.datediff(F.col("DATA_AZIONE"), F.col("DATA_BOLLA"))
            ).otherwise(F.lit(None).cast("int"))
        )
        # FLG_RITARDO: ricavato confrontando DATA_AZIONE (proxy consegna) con
        # DATA_CONSEGNA_PREV. Non esiste un flag ritardo nativo (SP_FLAGRITARDO inesistente).
        .withColumn(
            "FLG_RITARDO",
            F.when(
                F.col("DATA_AZIONE").isNotNull() & F.col("DATA_CONSEGNA_PREV").isNotNull(),
                F.col("DATA_AZIONE") > F.col("DATA_CONSEGNA_PREV")
            ).otherwise(F.lit(None).cast("boolean"))
        )
        .withColumn("_silver_ts", F.current_timestamp())
    )

    rows_clean = silver_df.count()
    ritardi_cnt = silver_df.filter(F.col("FLG_RITARDO") == True).count()
    logger.info(f"Righe silver dopo deduplica: {rows_clean} | con ritardo (proxy): {ritardi_cnt}")
    check_row_count(silver_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    if not spark.catalog.tableExists(TARGET_TABLE):
        logger.info(f"Prima esecuzione — CTAS {TARGET_TABLE}")
        silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)
    else:
        DeltaTable.forName(spark, TARGET_TABLE).alias("tgt").merge(
            silver_df.alias("src"),
            "tgt.TRASPORTO_ID = src.TRASPORTO_ID "
            "AND tgt.SITO_COD = src.SITO_COD "
            "AND tgt.DATA_BOLLA = src.DATA_BOLLA "
            "AND tgt.BOLLA_NRO = src.BOLLA_NRO"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    logger.info(f"END {NOTEBOOK_NAME} | righe_lette={rows_read} | righe_silver={rows_clean} | ritardi={ritardi_cnt}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
