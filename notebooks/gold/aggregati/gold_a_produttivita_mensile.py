# Databricks notebook source
# Area: Aggregati DataMart — Gold
# Versione: 3.0.0  Data: 2026-06-08
# Tabella: A_PRODUTTIVITA_MENSILE (schema gold_prod.logistica_dm)
# Sorgente: gold_prod.logistica.F_PREP_SPED — solo GROUP BY + funzioni aggregate.
# Grain: SITO_COD + ANNO_MESE.
# Misure su CARTONI/QUINTALI (NON colli, non disponibili — OP-27). Produttività cartoni/ora.

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

NOTEBOOK_NAME = "gold_a_produttivita_mensile"
GOLD_CATALOG  = get_catalog("gold", env)
# RICABLATO (standard 2-notebook): la produttivita' e' ora un fatto dedicato,
# non piu' F_PREP_SPED (tornato alla grana prelievo legacy).
SOURCE_TABLE  = f"{GOLD_CATALOG}.logistica.F_TURNO_PREP_SITO"
TARGET_TABLE  = f"{GOLD_CATALOG}.logistica_dm.A_PRODUTTIVITA_MENSILE"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | ANNO_MESE={ANNO_MESE}")

    df = (spark.read.table(SOURCE_TABLE)
          .filter(F.date_format(F.col("DATA_PREPARAZ"), "yyyyMM") == F.lit(ANNO_MESE))
          .withColumn("ANNO_MESE", F.lit(ANNO_MESE)))

    agg = (df.groupBy("SITO_COD", "ANNO_MESE")
        .agg(
            F.sum("TOT_CARTONI_PREP").alias("TOT_CARTONI_PREP"),
            F.sum("TOT_CARTONI").alias("TOT_CARTONI"),
            F.sum("TOT_CARTONI_INEVASI").alias("TOT_CARTONI_INEVASI"),
            F.sum("TOT_QUINTALI_PREP").alias("TOT_QUINTALI_PREP"),
            F.sum("TOT_QUINTALI").alias("TOT_QUINTALI"),
            F.sum("ORE_LAVORATE").alias("ORE_LAVORATE_TOT"),
            F.sum("ORE_PRODUTTIVE").alias("ORE_PRODUTTIVE_TOT"),
            F.countDistinct("PREPARATORE_COD").alias("OPERATORI_DISTINTI"),
            F.avg("PRODUTTIVITA_CARTONI_ORA").alias("PRODUTTIVITA_CARTONI_ORA_MEDIA"),
            F.max("PRODUTTIVITA_CARTONI_ORA").alias("PRODUTTIVITA_CARTONI_ORA_MAX"),
            F.expr("percentile_approx(PRODUTTIVITA_CARTONI_ORA, 0.5)").alias("PRODUTTIVITA_CARTONI_ORA_MEDIANA"),
            F.count("RIEPILOGO_NRO").alias("NUM_RIEPILOGHI"),
        )
        .withColumn("PRODUTTIVITA_CARTONI_ORA_AGGR",
            F.when(F.col("ORE_PRODUTTIVE_TOT") > 0,
                   F.col("TOT_CARTONI_PREP") / F.col("ORE_PRODUTTIVE_TOT")))
        .withColumn("PERC_ORE_ATTREZZAGGIO",
            F.when(F.col("ORE_LAVORATE_TOT") > 0,
                   (F.col("ORE_LAVORATE_TOT") - F.col("ORE_PRODUTTIVE_TOT"))
                   / F.col("ORE_LAVORATE_TOT") * F.lit(100.0)))
    )

    out = agg.select(
        F.col("SITO_COD").cast(StringType()),
        F.col("ANNO_MESE").cast(StringType()),
        F.col("TOT_CARTONI_PREP").cast(DoubleType()),
        F.col("TOT_CARTONI").cast(DoubleType()),
        F.col("TOT_CARTONI_INEVASI").cast(DoubleType()),
        F.col("TOT_QUINTALI_PREP").cast(DoubleType()),
        F.col("TOT_QUINTALI").cast(DoubleType()),
        F.col("ORE_LAVORATE_TOT").cast(DoubleType()),
        F.col("ORE_PRODUTTIVE_TOT").cast(DoubleType()),
        F.col("OPERATORI_DISTINTI").cast(LongType()),
        F.col("PRODUTTIVITA_CARTONI_ORA_MEDIA").cast(DoubleType()),
        F.col("PRODUTTIVITA_CARTONI_ORA_MAX").cast(DoubleType()),
        F.col("PRODUTTIVITA_CARTONI_ORA_MEDIANA").cast(DoubleType()),
        F.col("PRODUTTIVITA_CARTONI_ORA_AGGR").cast(DoubleType()),
        F.col("PERC_ORE_ATTREZZAGGIO").cast(DoubleType()),
        F.col("NUM_RIEPILOGHI").cast(LongType()),
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
