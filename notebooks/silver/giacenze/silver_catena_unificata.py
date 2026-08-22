# Databricks notebook source
# Area: Giacenze / Stock (migrazione TO-BE)
# Layer: Silver (elaborazione intermedia)
# Versione: 1.0.0
# Data: 2026-06-09
# Descrizione: Replica TO-BE di WL2_CATENA = CATENA UNION CATENA_ESTERNI.
#              SORGENTI: silver.logistica.catena_clean + silver.logistica.catena_esterni_clean
#                        (entrambe gia' pulite con schema omogeneo CATE_*).
#              ANOMALIA LEGACY ST15 (NON replicata): l'AS-IS fa UNION su TUPLA INTERA, per cui
#              record con stessa chiave logica ma attributi divergenti SOPRAVVIVONO entrambi
#              (rischio gonfiaggio giacenze). Qui: dedup ESPLICITA per CHIAVE LOGICA con precedenza.
#
#              CHIAVE LOGICA dedup: MAG_SITO_COD + CATE_NRO_ETICHETTA + locazione
#                                   (CATE_CORSIA, CATE_COLONNA, CATE_PIANO, CATE_LIVELLO).
#                                   L'etichetta identifica univocamente il pallet/UDC; la locazione
#                                   completa la chiave per i casi di etichetta riutilizzata.
#              REGOLA DI PRECEDENZA: catena INTERNA (_origine='CATENA') prevale su catena_esterni.
#                                   A parita' di origine, prevale la rilevazione piu' recente (ETL_DATINS DESC,
#                                   poi _silver_ts DESC come tie-break deterministico).
#              Riferimento: Linee guida §4 (CATENA), §6 anomalia WL2_CATENA UNION; §9-bis ST15.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME   = "silver_catena_unificata"
SILVER_CATALOG  = get_catalog("silver", env)
SCHEMA          = "logistica"
SOURCE_CATENA   = f"{SILVER_CATALOG}.{SCHEMA}.catena_clean"
SOURCE_ESTERNI  = f"{SILVER_CATALOG}.{SCHEMA}.catena_esterni_clean"
TARGET_TABLE    = f"{SILVER_CATALOG}.{SCHEMA}.catena_unificata"

# Chiave logica e precedenza
LOGICAL_KEY = ["MAG_SITO_COD", "CATE_NRO_ETICHETTA", "CATE_CORSIA", "CATE_COLONNA", "CATE_PIANO", "CATE_LIVELLO"]
# Precedenza origine: piu' basso = vince
ORIGINE_RANK = {"CATENA": 0, "CATENA_ESTERNI": 1}

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    if not spark.catalog.tableExists(SOURCE_CATENA):
        logger.warning(f"Sorgente {SOURCE_CATENA} non esiste. Notebook terminato.")
        dbutils.notebook.exit("NO_SOURCE")

    ca = (spark.table(SOURCE_CATENA)
          .filter(F.col("_bronze_load_date") == F.lit(run_date))
          .withColumn("_origine", F.lit("CATENA")))

    if spark.catalog.tableExists(SOURCE_ESTERNI):
        ce = (spark.table(SOURCE_ESTERNI)
              .filter(F.col("_bronze_load_date") == F.lit(run_date))
              .withColumn("_origine", F.lit("CATENA_ESTERNI")))
        unione = ca.unionByName(ce, allowMissingColumns=True)
    else:
        logger.warning(f"{SOURCE_ESTERNI} non esiste: uso solo CATENA.")
        unione = ca

    rows_union = unione.count()
    logger.info(f"Righe union (pre-dedup): {rows_union}")
    if rows_union == 0:
        logger.warning("Nessun dato. Notebook terminato.")
        dbutils.notebook.exit("NO_DATA")

    # ── Dedup esplicita per chiave logica + precedenza (correzione ST15) ──────
    _rank_expr = F.create_map([F.lit(x) for kv in ORIGINE_RANK.items() for x in kv])
    order_cols = [_rank_expr[F.col("_origine")].asc()]   # CATENA prevale su esterni
    # rilevazione piu' recente: ETL_DATINS se presente (alcune sorgenti non ce l'hanno),
    # altrimenti CATE_DATA_CARICO; tie-break deterministico su _silver_ts.
    if "ETL_DATINS" in unione.columns:
        order_cols.append(F.col("ETL_DATINS").desc_nulls_last())
    elif "CATE_DATA_CARICO" in unione.columns:
        order_cols.append(F.col("CATE_DATA_CARICO").desc_nulls_last())
    order_cols.append(F.col("_silver_ts").desc_nulls_last())
    w = Window.partitionBy(*[F.col(c) for c in LOGICAL_KEY]).orderBy(*order_cols)

    dedup_df = (
        unione
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("_silver_unif_ts", F.current_timestamp())
    )

    check_not_null(dedup_df, ["MAG_SITO_COD", "CATE_NRO_ETICHETTA"], NOTEBOOK_NAME)
    rows_dedup = dedup_df.count()
    logger.info(f"Righe dopo dedup per chiave logica: {rows_dedup} (rimosse {rows_union - rows_dedup} ambiguita' ST15)")
    check_row_count(dedup_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    # Idempotente: dynamic partition overwrite (no raddoppio su re-run stesso giorno).
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (dedup_df.write.format("delta").mode("overwrite")
     .partitionBy("_bronze_load_date").option("mergeSchema", "true")
     .saveAsTable(TARGET_TABLE))
    logger.info(f"SNAPSHOT (dyn overwrite) {TARGET_TABLE} ({rows_dedup} righe per run_date={run_date})")

    logger.info(f"END {NOTEBOOK_NAME} | righe_union={rows_union} | righe_dedup={rows_dedup}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
