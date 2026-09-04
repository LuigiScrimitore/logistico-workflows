# Databricks notebook source
# Area: Aggregati DataMart — Gold
# Versione: 3.0.0  Data: 2026-06-08
# Tabella: A_TURNO_PREP_SITO (schema gold_prod.logistica_dm)
# Sorgente: gold_prod.logistica.F_PREP_SPED — GROUP BY giornaliero per sito.
# Grain: SITO_COD + DATA_PREPARAZ.
# NOTA: il "turno" come attributo distinto non e' presente nei dati reali (OP); grain corrente per giornata.

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from utils import get_catalog
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType, DoubleType, LongType
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME = "gold_a_turno_prep_sito"
GOLD_CATALOG  = get_catalog("gold", env)
# RICABLATO (standard 2-notebook): la produttivita'/turno e' ora un fatto dedicato,
# non piu' F_PREP_SPED (che e' tornato alla grana prelievo legacy).
SOURCE_TABLE  = f"{GOLD_CATALOG}.logistica.F_TURNO_PREP_SITO"
TARGET_TABLE  = f"{GOLD_CATALOG}.logistica_dm.A_TURNO_PREP_SITO"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    df = (spark.read.table(SOURCE_TABLE)
          .filter(F.col("DATA_PREPARAZ") == F.lit(run_date).cast(DateType())))

    out = (df.groupBy("SITO_COD", "DATA_PREPARAZ")
        .agg(
            F.sum("TOT_CARTONI_PREP").cast(DoubleType()).alias("TOT_CARTONI_PREP"),
            F.sum("TOT_QUINTALI_PREP").cast(DoubleType()).alias("TOT_QUINTALI_PREP"),
            F.sum("ORE_LAVORATE").cast(DoubleType()).alias("ORE_LAVORATE_TOT"),
            F.sum("ORE_PRODUTTIVE").cast(DoubleType()).alias("ORE_PRODUTTIVE_TOT"),
            F.countDistinct("PREPARATORE_COD").cast(LongType()).alias("OPERATORI_ATTIVI"),
            F.countDistinct("RIEPILOGO_NRO").cast(LongType()).alias("NUM_RIEPILOGHI"),
            F.avg("PRODUTTIVITA_CARTONI_ORA").cast(DoubleType()).alias("PRODUTTIVITA_CARTONI_ORA_MEDIA"),
        )
        .withColumn("PERC_ORE_ATTREZZAGGIO",
            F.when(F.col("ORE_LAVORATE_TOT") > 0,
                   (F.col("ORE_LAVORATE_TOT") - F.col("ORE_PRODUTTIVE_TOT"))
                   / F.col("ORE_LAVORATE_TOT") * F.lit(100.0)).cast(DoubleType()))
        .withColumn("DWH_UPDATED_AT", F.current_timestamp())
        .select(
            F.col("SITO_COD").cast(StringType()),
            F.col("DATA_PREPARAZ").cast(DateType()),
            F.col("TOT_CARTONI_PREP"),
            F.col("TOT_QUINTALI_PREP"),
            F.col("ORE_LAVORATE_TOT"),
            F.col("ORE_PRODUTTIVE_TOT"),
            F.col("OPERATORI_ATTIVI"),
            F.col("NUM_RIEPILOGHI"),
            F.col("PRODUTTIVITA_CARTONI_ORA_MEDIA"),
            F.col("PERC_ORE_ATTREZZAGGIO"),
            F.col("DWH_UPDATED_AT"),
        )
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.logistica_dm")
    (out.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"DATA_PREPARAZ = '{run_date}'")
        .option("mergeSchema", "true")
        .partitionBy("DATA_PREPARAZ")
        .saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | righe={out.count()} | target={TARGET_TABLE}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
