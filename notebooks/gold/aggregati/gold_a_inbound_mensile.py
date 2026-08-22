# Databricks notebook source
# Area: Aggregati DataMart — Gold
# Versione: 4.0.0  Data: 2026-07-04
# Tabella: A_INBOUND_MENSILE (schema gold_prod.logistica_dm)
# Sorgente: gold_prod.logistica.F_CARICO — solo GROUP BY + funzioni aggregate.
# Grain: FORNITORE_COD + SITO_COD + ANNO_MESE.
#
# ⚠️ RIALLINEATO a F_CARICO v4.0 (grain ETICHETTA — silver.logistica_curated.carico).
#   La v3.0 leggeva colonne del vecchio grain "riga dettaglio" ora INESISTENTI
#   (QTA_ORDINATA, NRO_PZ_CARICATI, SCARTO_QTA, PESO_LORDO, FLAG_SCARTO, CARICO_NRO):
#   avrebbe dato AnalysisException. Misure ora derivate dalle colonne reali del fact.
#   Note: QTA_ORDINATA_TOT = pezzi ordinati = SUM(QTA_ORD_FORN[colli] × NUM_PZ_IMB_ORD_FORN);
#         omogenea con QTA_CARICO_TOT (pezzi) per un ammanco corretto (OP-CAR-3 + fix unità);
#         VAL_COSTO_CARICO non aggregata (=NULL, OP-CAR-1 sorgente dismessa).

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from utils import get_catalog
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, LongType
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")
ANNO_MESE = run_date[:4] + run_date[5:7]

# COMMAND ----------

NOTEBOOK_NAME = "gold_a_inbound_mensile"
GOLD_CATALOG  = get_catalog("gold", env)
SOURCE_TABLE  = f"{GOLD_CATALOG}.logistica.F_CARICO"
TARGET_TABLE  = f"{GOLD_CATALOG}.logistica_dm.A_INBOUND_MENSILE"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | ANNO_MESE={ANNO_MESE}")

    df = spark.read.table(SOURCE_TABLE).filter(F.col("ANNO_MESE") == F.lit(ANNO_MESE))

    agg = (df.groupBy("FORNITORE_COD", "SITO_COD", "ANNO_MESE")
        .agg(
            F.sum("QTA_CARICO").alias("QTA_CARICO_TOT"),
            F.sum("QTA_UF_CARICO").alias("QTA_UF_CARICO_TOT"),
            F.sum("PES_CARICO").alias("PESO_CARICO_TOT"),
            F.sum("VOL_CARICO").alias("VOL_CARICO_TOT"),
            F.sum("NUM_PLT_CARICO").alias("NUM_PLT_TOT"),
            F.sum("NUM_IMB_CARICO").alias("NUM_IMB_TOT"),
            # OP-CAR-3: quantità ordinata in PEZZI. QTA_ORD_FORN è in COLLI (sulla prima
            # etichetta del gruppo → SUM = colli ordinati); si converte in pezzi con
            # NUM_PZ_IMB_ORD_FORN (pz/collo) per essere OMOGENEA con QTA_CARICO (pezzi ricevuti).
            # Verificato: SUM(QTA_ORD_FORN×NUM_PZ_IMB_ORD_FORN) ≈ SUM(QTA_CARICO) (ammanco ~1.5%).
            F.sum(F.col("QTA_ORD_FORN") * F.coalesce(F.col("NUM_PZ_IMB_ORD_FORN"), F.lit(1)))
             .alias("QTA_ORDINATA_TOT"),
            F.countDistinct("NUM_DOC_CARICO").alias("NUM_CARICHI"),
            F.count(F.lit(1)).alias("NUM_ETICHETTE"),
        )
        # AMMANCO = pezzi ordinati − pezzi ricevuti (misura di business, non scarto-di-record).
        .withColumn("AMMANCO_QTA_TOT", F.col("QTA_ORDINATA_TOT") - F.col("QTA_CARICO_TOT"))
        .withColumn("TASSO_AMMANCO",
                    F.when(F.col("QTA_ORDINATA_TOT") > 0,
                           (F.col("QTA_ORDINATA_TOT") - F.col("QTA_CARICO_TOT")) / F.col("QTA_ORDINATA_TOT")))
    )

    out = agg.select(
        F.col("FORNITORE_COD").cast(StringType()),
        F.col("SITO_COD").cast(StringType()),
        F.col("ANNO_MESE").cast(StringType()),
        F.col("QTA_ORDINATA_TOT").cast(DoubleType()),
        F.col("QTA_CARICO_TOT").cast(DoubleType()),
        F.col("QTA_UF_CARICO_TOT").cast(DoubleType()),
        F.col("AMMANCO_QTA_TOT").cast(DoubleType()),
        F.col("TASSO_AMMANCO").cast(DoubleType()),
        F.col("PESO_CARICO_TOT").cast(DoubleType()),
        F.col("VOL_CARICO_TOT").cast(DoubleType()),
        F.col("NUM_PLT_TOT").cast(DoubleType()),
        F.col("NUM_IMB_TOT").cast(DoubleType()),
        F.col("NUM_CARICHI").cast(LongType()),
        F.col("NUM_ETICHETTE").cast(LongType()),
        F.current_timestamp().alias("DWH_UPDATED_AT"),
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
