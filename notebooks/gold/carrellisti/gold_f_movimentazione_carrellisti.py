# Databricks notebook source
# Area: Carrellisti — Gold (Fact)
# Versione: 3.1.0  Data: 2026-07-05
#   v3.1: aggiunta misura NUM_PLT_MOVIMENTATI (pallet movimentati/giorno; 2 se DOPPIO_MOVIM='SI')
#         allineata a CDT_DW F_MOV_CARR.NUM_PLT_MOV_CARR — colma il gap misura mantenendo la
#         grana giornaliera (certifica 2026-07-05). Grana per-movimento + rifiutati = OP-MOV-1.
# Tabella: F_MOVIMENTAZIONE_CARRELLISTI
# Grain: (CARRELLISTA_COD, DATA_PRESENZA, SITO_COD).
#
# Sorgenti Silver (colonne reali OP-27):
#   sessione_carrellista (presenza): CARRELLISTA_COD, DATA_PRESENZA, SITO_COD, ORA_LOGIN, ORA_LOGOUT,
#                                    ORE_PRESENZA, FLG_SESSIONE_APERTA.
#   missione_carrellista:            CARRELLISTA_COD, SITO_COD, DATA_RICH_ABB, ORA_RICH_ABB,
#                                    DATA_EFFETTIVA_INIZIO, DATA_EFFETTIVA_FINE,
#                                    CARICO_NRO, ORDINE_NRO, MISSIONE_COD, GABBIA_NRO, PICKING_NRO,
#                                    CORSIA_PARTENZA/ARRIVO, COLONNA_*, PIANO_*, LIVELLO_*.
#
# IMPORTANTE OP-27: il Silver sessione NON ha ORE_PRODUTTIVE (cartellino non ha pause/riunioni).
#                  Niente TIPO_MISSIONE (non esiste). Le missioni si aggregano per CARRELLISTA/DATA/SITO.
# Pattern: replaceWhere su DATA_PRESENZA.

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from utils import get_catalog
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, DateType, IntegerType
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME    = "gold_f_movimentazione_carrellisti"
SILVER_CATALOG   = get_catalog("silver", env)
GOLD_CATALOG     = get_catalog("gold",   env)
SRC_SESSIONE     = f"{SILVER_CATALOG}.logistica.sessione_carrellista"
SRC_MISSIONI     = f"{SILVER_CATALOG}.logistica.missione_carrellista"
TARGET_TABLE     = f"{GOLD_CATALOG}.logistica.F_MOVIMENTAZIONE_CARRELLISTI"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    df_sess = (spark.read.table(SRC_SESSIONE)
               .filter(F.col("DATA_PRESENZA") == F.lit(run_date).cast(DateType())))

    df_miss = (spark.read.table(SRC_MISSIONI)
               .filter(F.col("DATA_RICH_ABB") == F.lit(run_date).cast(DateType())))

    # Aggregazione missioni per (CARRELLISTA_COD, DATA, SITO_COD)
    df_miss_agg = (df_miss
        .withColumn("DURATA_SEC",
            F.when(F.col("DATA_EFFETTIVA_INIZIO").isNotNull() & F.col("DATA_EFFETTIVA_FINE").isNotNull(),
                   F.unix_timestamp("DATA_EFFETTIVA_FINE") - F.unix_timestamp("DATA_EFFETTIVA_INIZIO")))
        .groupBy("CARRELLISTA_COD", "DATA_RICH_ABB", "SITO_COD")
        .agg(
            F.count("*").alias("NUM_MISSIONI"),
            F.countDistinct("CARICO_NRO").alias("NUM_CARICHI"),
            F.countDistinct("RIEPILOGO_NRO").alias("NUM_RIEPILOGHI"),
            (F.sum("DURATA_SEC") / 60.0).alias("DURATA_TOT_MIN"),
            # NUM_PLT movimentati (allineato a CDT_DW F_MOV_CARR.NUM_PLT_MOV_CARR, certifica 2026-07-05):
            # ogni missione = 1 pallet, 2 se DOPPIO_MOVIM='SI'. Colma il gap misura vs CDT_DW
            # mantenendo la grana giornaliera (grana per-movimento = estensione futura, OP-MOV-1).
            F.sum(F.when(F.upper(F.trim(F.col("DOPPIO_MOVIM"))) == F.lit("SI"), F.lit(2))
                   .otherwise(F.lit(1))).alias("NUM_PLT_MOVIMENTATI"),
        )
        .withColumnRenamed("DATA_RICH_ABB", "DATA_PRESENZA")
    )

    # Join sessione (presenza) ⋈ missioni aggregate
    fact = (df_sess.alias("s")
        .join(df_miss_agg.alias("m"),
              (F.col("s.CARRELLISTA_COD") == F.col("m.CARRELLISTA_COD")) &
              (F.col("s.DATA_PRESENZA") == F.col("m.DATA_PRESENZA")) &
              (F.col("s.SITO_COD") == F.col("m.SITO_COD")),
              "left")
        .select(
            F.col("s.CARRELLISTA_COD").cast(StringType()).alias("CARRELLISTA_COD"),
            F.col("s.DATA_PRESENZA").cast(DateType()).alias("DATA_PRESENZA"),
            F.col("s.SITO_COD").cast(StringType()).alias("SITO_COD"),
            F.col("s.ORA_LOGIN").alias("ORA_LOGIN"),
            F.col("s.ORA_LOGOUT").alias("ORA_LOGOUT"),
            F.col("s.ORE_PRESENZA").cast(DoubleType()).alias("ORE_PRESENZA"),
            F.col("s.FLG_SESSIONE_APERTA").cast(StringType()).alias("FLG_SESSIONE_APERTA"),
            F.coalesce(F.col("m.NUM_MISSIONI"), F.lit(0)).cast(IntegerType()).alias("NUM_MISSIONI"),
            F.coalesce(F.col("m.NUM_CARICHI"), F.lit(0)).cast(IntegerType()).alias("NUM_CARICHI"),
            F.coalesce(F.col("m.NUM_RIEPILOGHI"), F.lit(0)).cast(IntegerType()).alias("NUM_RIEPILOGHI"),
            F.coalesce(F.col("m.NUM_PLT_MOVIMENTATI"), F.lit(0)).cast(IntegerType()).alias("NUM_PLT_MOVIMENTATI"),
            F.col("m.DURATA_TOT_MIN").cast(DoubleType()).alias("DURATA_TOT_MIN"),
            F.current_timestamp().alias("DWH_UPDATED_AT"),
        )
    )

    rows = fact.count()
    logger.info(f"F_MOVIMENTAZIONE_CARRELLISTI righe per DATA_PRESENZA={run_date}: {rows}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_CATALOG}.logistica")
    (fact.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"DATA_PRESENZA = '{run_date}'")
        .option("mergeSchema", "true")
        .partitionBy("DATA_PRESENZA")
        .saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | righe_scritte={rows} | target={TARGET_TABLE}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
