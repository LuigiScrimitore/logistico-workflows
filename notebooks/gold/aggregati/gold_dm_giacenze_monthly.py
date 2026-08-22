# Databricks notebook source
# Area: Aggregati DataMart — Gold
# Versione: 3.0.0  Data: 2026-06-08
# Tabella: A_GIACENZE_MONTHLY (schema gold_prod.logistica_dm)
# Sorgente: gold_prod.logistica.F_GIACENZE_DAILY — solo aggregazioni.
# Grain: ART_RADICE + MAG_COD + ANNO_MESE.
# Misure: avg/max/min/fine-mese su QTA_PEZZI, QTA_UF, QTA_IN_SCADENZA + INDICE_ROTAZIONE + VARIAZIONE.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from utils import get_catalog
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")
ANNO_MESE = run_date[:4] + run_date[5:7]

# COMMAND ----------

NOTEBOOK_NAME = "gold_a_giacenze_monthly"
GOLD_CATALOG  = get_catalog("gold", env)
SOURCE_TABLE  = f"{GOLD_CATALOG}.logistica.F_GIACENZE_DAILY"
TARGET_TABLE  = f"{GOLD_CATALOG}.logistica_dm.A_GIACENZE_MONTHLY"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | ANNO_MESE={ANNO_MESE}")

    df = (spark.read.table(SOURCE_TABLE)
          .filter(F.date_format(F.col("DATA_FOTO"), "yyyyMM") == F.lit(ANNO_MESE)))

    # Ultima data del mese per QTA_FINE_MESE
    max_date_row = df.agg(F.max("DATA_FOTO").alias("md")).collect()
    if not max_date_row or max_date_row[0]["md"] is None:
        logger.info("Nessuna giacenza nel mese — uscita graceful.")
        dbutils.notebook.exit("NO_DATA")
    max_date = max_date_row[0]["md"]

    df_fm = (df.filter(F.col("DATA_FOTO") == F.lit(max_date))
             .select(
                 F.col("ART_RADICE"),
                 F.col("MAG_COD"),
                 F.col("QTA_PEZZI").alias("QTA_PEZZI_FINE_MESE"),
                 F.col("QTA_UF").alias("QTA_UF_FINE_MESE"),
             ))

    agg = (df.groupBy("ART_RADICE", "MAG_COD")
        .agg(
            F.avg("QTA_PEZZI").alias("AVG_QTA_PEZZI"),
            F.max("QTA_PEZZI").alias("MAX_QTA_PEZZI"),
            F.min("QTA_PEZZI").alias("MIN_QTA_PEZZI"),
            F.avg("QTA_UF").alias("AVG_QTA_UF"),
            F.avg("QTA_IN_SCADENZA").alias("AVG_QTA_IN_SCADENZA"),
            F.max("QTA_IN_SCADENZA").alias("MAX_QTA_IN_SCADENZA"),
        )
        .withColumn("ANNO_MESE", F.lit(ANNO_MESE))
    )

    out = (agg.alias("a")
        .join(df_fm.alias("fm"),
              (F.col("a.ART_RADICE") == F.col("fm.ART_RADICE")) &
              (F.col("a.MAG_COD") == F.col("fm.MAG_COD")), "left")
        .withColumn("VARIAZIONE_QTA_PEZZI", F.col("MAX_QTA_PEZZI") - F.col("MIN_QTA_PEZZI"))
        .withColumn("INDICE_ROTAZIONE",
            F.when((F.col("AVG_QTA_PEZZI").isNotNull()) & (F.col("AVG_QTA_PEZZI") > 0),
                   F.col("QTA_PEZZI_FINE_MESE") / F.col("AVG_QTA_PEZZI")))
        .select(
            F.col("a.ART_RADICE").cast(StringType()).alias("ART_RADICE"),
            F.col("a.MAG_COD").cast(StringType()).alias("MAG_COD"),
            F.col("a.ANNO_MESE").cast(StringType()).alias("ANNO_MESE"),
            F.col("AVG_QTA_PEZZI").cast(DoubleType()),
            F.col("MAX_QTA_PEZZI").cast(DoubleType()),
            F.col("MIN_QTA_PEZZI").cast(DoubleType()),
            F.col("QTA_PEZZI_FINE_MESE").cast(DoubleType()),
            F.col("VARIAZIONE_QTA_PEZZI").cast(DoubleType()),
            F.col("AVG_QTA_UF").cast(DoubleType()),
            F.col("QTA_UF_FINE_MESE").cast(DoubleType()),
            F.col("AVG_QTA_IN_SCADENZA").cast(DoubleType()),
            F.col("MAX_QTA_IN_SCADENZA").cast(DoubleType()),
            F.col("INDICE_ROTAZIONE").cast(DoubleType()),
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
