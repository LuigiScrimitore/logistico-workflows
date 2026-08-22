# Databricks notebook source
# Area: Preparazione Spedizioni — Fact "produttivita'/turno"
# Layer: Silver PREP (Fase 1 modellazione + Fase 2 calcolo)  →  silver.logistica_curated.turno_prep_sito
# Versione: 1.0.0
# Data: 2026-06-10
# Descrizione: STRATO PREP (standard 2-notebook, Linee guida §1-bis).
#              Ospita la logica di PRODUTTIVITA' che prima viveva (impropriamente) dentro
#              gold_f_prep_sped v3. Grana = riepilogo per (SITO, DATA_PREPARAZ, PREPARATORE, RIEPILOGO).
#              NON e' il F_PREP_SPED legacy (quello e' grana prelievo) → fatto separato F_TURNO_PREP_SITO.
#
#              SORGENTE (silver.clean): silver.logistica.prep_riepilogo (riepiloghi STAT puliti).
#
#              FASE 2 (calcolo):
#                - TS_INIZIO/TS_FINE da DATA_*_PREP + ORA_*_PREP (HHMM senza zero-pad → lpad 4)
#                - ORE_LAVORATE = (fine - inizio)/3600
#                - REGOLA 30 MIN ATTREZZAGGIO: prima sessione del giorno per (PREP, DATA, SITO):
#                    ORE_PRODUTTIVE = max(0, ORE_LAVORATE - 0.5); altrimenti = ORE_LAVORATE
#                - PRODUTTIVITA_CARTONI_ORA = TOT_CARTONI_PREP / ORE_PRODUTTIVE
#              Le surrogate key dimensionali NON si fanno qui: vivono nel Gold (Fase 3).
#              MODE: FULL_OVERWRITE (la finestra-giorno la gestisce il Gold/aggregati).

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from dq_helper import check_row_count
from utils import get_catalog
from pyspark.sql import functions as F, Window
from pyspark.sql.types import DoubleType, BooleanType
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME  = "silver_prep_turno_prep_sito"
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.prep_riepilogo"
TARGET_TABLE   = f"{SILVER_CATALOG}.logistica_curated.turno_prep_sito"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    if not spark.catalog.tableExists(SOURCE_TABLE):
        logger.warning(f"Sorgente {SOURCE_TABLE} non esiste. Notebook terminato.")
        dbutils.notebook.exit("NO_SOURCE")

    src = spark.read.table(SOURCE_TABLE)
    rows_src = src.count()
    logger.info(f"Righe lette da {SOURCE_TABLE}: {rows_src}")
    if rows_src == 0:
        dbutils.notebook.exit("NO_DATA")

    # ── FASE 2: timestamp inizio/fine (ORA_*_PREP intero HHMM senza zero-pad) ──
    def to_ts(date_col, ora_col):
        ora_str = F.col(ora_col).cast("string")
        ora_padded = F.lpad(F.regexp_replace(ora_str, "[^0-9]", ""), 4, "0")
        return F.coalesce(
            F.to_timestamp(F.concat_ws(" ", F.col(date_col).cast("string"), ora_str),
                           "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp(F.concat_ws(" ", F.col(date_col).cast("string"), ora_padded),
                           "yyyy-MM-dd HHmm"),
        )

    enriched = (src
        .withColumn("TS_INIZIO", to_ts("DATA_INIZIO_PREP", "ORA_INIZIO_PREP"))
        .withColumn("TS_FINE",   to_ts("DATA_FINE_PREP",   "ORA_FINE_PREP"))
        .withColumn("ORE_LAVORATE",
                    F.when(F.col("TS_INIZIO").isNotNull() & F.col("TS_FINE").isNotNull(),
                           (F.unix_timestamp("TS_FINE") - F.unix_timestamp("TS_INIZIO")) / 3600.0))
        .withColumn("FLAG_TEMPO_ASSENTE", F.col("ORE_LAVORATE").isNull().cast(BooleanType()))
    )

    # Regola 30 min attrezzaggio: prima sessione del giorno per (PREP, DATA, SITO)
    w_turno = (Window
               .partitionBy("PREPARATORE_COD", "DATA_PREPARAZ", "SITO_COD")
               .orderBy(F.col("TS_INIZIO").asc_nulls_last()))
    prep_df = (enriched
        .withColumn("_seq", F.row_number().over(w_turno))
        .withColumn("ORE_PRODUTTIVE",
            F.when(F.col("ORE_LAVORATE").isNull(), F.lit(None).cast(DoubleType()))
             .when(F.col("_seq") == 1, F.greatest(F.lit(0.0), F.col("ORE_LAVORATE") - F.lit(0.5)))
             .otherwise(F.col("ORE_LAVORATE")))
        .withColumn("PRODUTTIVITA_CARTONI_ORA",
            F.when((F.col("ORE_PRODUTTIVE").isNotNull()) & (F.col("ORE_PRODUTTIVE") > 0),
                   F.col("TOT_CARTONI_PREP").cast(DoubleType()) / F.col("ORE_PRODUTTIVE"))
             .otherwise(F.lit(None).cast(DoubleType())))
        .drop("_seq")
        .withColumn("_silver_ts", F.current_timestamp())
        .withColumn("_silver_load_date", F.lit(run_date).cast("date"))
    )

    rows_out = prep_df.count()
    logger.info(f"Righe turno_prep_sito: {rows_out}")
    check_row_count(prep_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_CATALOG}.logistica_curated")
    (prep_df.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(TARGET_TABLE))
    logger.info(f"FULL OVERWRITE {TARGET_TABLE} ({rows_out} righe)")

    logger.info(f"END {NOTEBOOK_NAME} | righe={rows_out}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
