# Databricks notebook source
# Area: Aggregati DataMart — Gold
# Versione: 3.0.0  Data: 2026-06-08
# Tabella: A_OUTBOUND_MENSILE (schema gold_prod.logistica_dm)
# Sorgenti: gold_prod.logistica.F_ORDINI + F_TRASPORTO — solo GROUP BY + funzioni aggregate.
# Grain: SITO_COD + CORRIERE_COD + ANNO_MESE.
# NB: F_ORDINI non ha quantità (sono nel dettaglio carico) -> conteggi. F_TRASPORTO (FASE 5)
#     è a grana bolla: porta trasporti/bolle/lead-time, NON QTA/COSTO (mantenuti NULL).

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from utils import get_catalog
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, LongType
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")
ANNO_MESE = run_date[:4] + run_date[5:7]

# COMMAND ----------

NOTEBOOK_NAME = "gold_a_outbound_mensile"
GOLD_CATALOG  = get_catalog("gold", env)
SRC_ORDINI    = f"{GOLD_CATALOG}.logistica.F_ORDINI"
SRC_TRASP     = f"{GOLD_CATALOG}.logistica.F_TRASPORTO"
TARGET_TABLE  = f"{GOLD_CATALOG}.logistica_dm.A_OUTBOUND_MENSILE"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | ANNO_MESE={ANNO_MESE}")

    df_o = spark.read.table(SRC_ORDINI).filter(F.col("ANNO_MESE") == F.lit(ANNO_MESE))
    # FASE 5: F_TRASPORTO ora a grana bolla. Rimappo i nomi al nuovo schema e normalizzo
    # le chiavi di join al naming ordini (MAG_SITO_COD->SITO_COD, VETTORE_SPED_COD->CORRIERE_COD).
    df_t = (spark.read.table(SRC_TRASP)
            .filter(F.date_format(F.col("DATA_BOLLA_SPED"), "yyyyMM") == F.lit(ANNO_MESE))
            .withColumn("ANNO_MESE", F.lit(ANNO_MESE))
            .withColumnRenamed("MAG_SITO_COD", "SITO_COD")
            .withColumnRenamed("VETTORE_SPED_COD", "CORRIERE_COD"))

    ord_agg = (df_o.groupBy("SITO_COD", "CORRIERE_COD", "ANNO_MESE")
        .agg(
            F.countDistinct("ORDINE_NRO").alias("NUM_ORDINI"),
            F.countDistinct("CARICO_NRO").alias("NUM_CARICHI"),
            F.countDistinct("FORNITORE_COD").alias("NUM_FORNITORI"),
            F.sum(F.when(F.col("FLAG_TRASFERITO") == "S", 1).otherwise(0)).alias("NUM_TRASFERITI"),
        )
    )

    # NB: a grana bolla F_TRASPORTO non porta QTA né COSTO (sono in prep_sped/carico).
    # QTA_TRASPORTATA_TOT e COSTO_STIMATO_EUR_TOT mantenuti NULL per stabilità schema datamart
    # (FASE 5: rivalutare se ricavarli da un'altra fact). Misure disponibili: trasporti/bolle/lead-time.
    trasp_agg = (df_t.groupBy("SITO_COD", "CORRIERE_COD", "ANNO_MESE")
        .agg(
            F.lit(None).cast(DoubleType()).alias("QTA_TRASPORTATA_TOT"),
            F.countDistinct("SP_ID").alias("NUM_TRASPORTI"),
            F.countDistinct("NUM_BOLLA_SPED").alias("NUM_BOLLE"),
            F.avg("LEAD_TIME_GG").alias("AVG_LEAD_TIME_GG"),
            F.lit(None).cast(DoubleType()).alias("COSTO_STIMATO_EUR_TOT"),
        )
    )

    out = (ord_agg.alias("o")
        .join(trasp_agg.alias("t"),
              (F.col("o.SITO_COD") == F.col("t.SITO_COD")) &
              (F.col("o.CORRIERE_COD") == F.col("t.CORRIERE_COD")) &
              (F.col("o.ANNO_MESE") == F.col("t.ANNO_MESE")),
              "full_outer")
        .select(
            F.coalesce(F.col("o.SITO_COD"), F.col("t.SITO_COD")).cast(StringType()).alias("SITO_COD"),
            F.coalesce(F.col("o.CORRIERE_COD"), F.col("t.CORRIERE_COD")).cast(StringType()).alias("CORRIERE_COD"),
            F.coalesce(F.col("o.ANNO_MESE"), F.col("t.ANNO_MESE")).cast(StringType()).alias("ANNO_MESE"),
            F.col("NUM_ORDINI").cast(LongType()),
            F.col("NUM_CARICHI").cast(LongType()),
            F.col("NUM_FORNITORI").cast(LongType()),
            F.col("NUM_TRASFERITI").cast(LongType()),
            F.col("QTA_TRASPORTATA_TOT").cast(DoubleType()),
            F.col("NUM_TRASPORTI").cast(LongType()),
            F.col("NUM_BOLLE").cast(LongType()),
            F.col("AVG_LEAD_TIME_GG").cast(DoubleType()),
            F.col("COSTO_STIMATO_EUR_TOT").cast(DoubleType()),
            F.current_timestamp().alias("DWH_UPDATED_AT"),
        )
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.logistica_dm")
    (out.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"ANNO_MESE = '{ANNO_MESE}'")
        .option("mergeSchema", "true")
        .partitionBy("ANNO_MESE")
        .saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | righe={out.count()} | target={TARGET_TABLE}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
