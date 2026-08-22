# Databricks notebook source
# ⚠️ DEPRECATO (2026-06-10, standard 2-notebook §1-bis).
# Sostituito da: notebooks/silver/trasporti/silver_prep_trasporto.py
#                → silver.logistica_curated.trasporto (consumato da gold_f_trasporto v4).
# Scriveva silver.logistica.t_trasp_mtv (non consumato dal Gold). NON eseguire.
import sys as _sys
try:
    dbutils.notebook.exit("DEPRECATED_USE_silver_prep_trasporto")  # noqa: F821
except Exception:
    _sys.exit(0)

# Area: CDT_ESTR (migrazione TO-BE) -> Trasporti / Movimenti
# Layer: Silver
# Versione: 1.0.0
# Data: 2026-06-09
# Descrizione: Replica logica di V_TRASP_MTV_CONS + V_TRASP_MTV_TRANSITO + SP_INS_T_TRASP_MTV.
#              SORGENTE (RIFATTA): silver.logistica.s_trasp_mtv ricostruita da
#                                  silver_trasp_mtv_build (NON più lo staging bronze).
#              MODE: DELTA_MERGE (movimenti)
#              Le 2 viste AS-IS sono complementari (CONS = MAG_SITO_TRANSITO_COD IS NULL,
#              TRANSITO = MAG_SITO_TRANSITO_COD IS NOT NULL): un UNION le copre entrambe.
#              Filtro comune: AZIONE_COD = 'S'.
#              Riferimento: CDT_ESTR_VISTE.sql righe 3881 / 3920.
#
# NOTA UDF (equivalenze TO-BE):
#  - FN_CLEAN_DAT_D(x) -> clean_dat_d / julian_to_date (logistica_utils). Qui le date
#    arrivano GIÀ pulite (DateType) dal build/silver_spedizioni_clean; fn_clean_dat_d
#    produce solo l'intero YYYYMMDD (chiave-giorno GIORNO_*_ID), non ri-pulisce.
#  - FN_GET_MAG_SITO_COD(sito, data) -> normalize_sito (già applicata in cleansing).
#    Qui passthrough: il sito è già canonico.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog, julian_to_date

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME  = "silver_t_trasp_mtv"
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
# RIFATTO: legge dal Silver elaborazione (build), non dallo staging bronze.
SOURCE_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.s_trasp_mtv"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.t_trasp_mtv"

logger = get_logger(NOTEBOOK_NAME)

# Helper: equivalente FN_CLEAN_DAT_D -> intero YYYYMMDD (chiave-giorno).
# Le date in ingresso sono già DateType (pulite a monte); coalesce su mista per robustezza.
def fn_clean_dat_d(col):
    as_date = F.when(
        col.cast("string").rlike(r"^[0-9]+$"), julian_to_date(col)
    ).otherwise(col.cast("date"))
    return F.coalesce(F.date_format(as_date, "yyyyMMdd").cast("int"), F.lit(0))

# Helper: passthrough per FN_GET_MAG_SITO_COD (TODO: lookup storico se necessario)
def fn_get_mag_sito_cod(sito_col, _date_col):
    return sito_col

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    if not spark.catalog.tableExists(SOURCE_TABLE):
        logger.warning(f"Sorgente {SOURCE_TABLE} non esiste (env di test vuoto). Notebook terminato.")
        dbutils.notebook.exit("NO_SOURCE")

    src = spark.table(SOURCE_TABLE).filter(F.col("AZIONE_COD") == F.lit("S"))
    rows_read = src.count()
    logger.info(f"Righe lette da {SOURCE_TABLE} (AZIONE_COD='S'): {rows_read}")
    if rows_read == 0:
        logger.warning("Nessun movimento da processare. Notebook terminato.")
        dbutils.notebook.exit("NO_DATA")

    # ── RAMO CONS (TRANSITO_COD IS NULL) → trasporto diretto CEDI→PDV o PDV→PDV ─
    cons = (
        src.filter(F.col("MAG_SITO_TRANSITO_COD").isNull())
        .select(
            fn_get_mag_sito_cod(F.col("MAG_SITO_COD"), F.col("DATA_AZIONE")).alias("MAG_SITO_COD"),
            F.coalesce(F.col("SOCIO_PREC_COD"), F.col("MAG_SITO_COD")).alias("TRASP_NODO_ORIG_COD"),
            F.col("SOCIO_COD").alias("TRASP_NODO_DEST_COD"),
            F.col("AUTISTA_SPED_COD"),
            F.col("AUTOMEZZO").alias("AUTOM_SPED_COD"),
            F.col("VETTORE").alias("VETTORE_SPED_COD"),
            F.col("SP_ID"),
            F.col("MTV_COD"),
            # DECODE(SOCIO_PREC_COD, NULL, 2, 3): 2=Da CEDI a PDV, 3=Da PDV a PDV
            F.when(F.col("SOCIO_PREC_COD").isNull(), F.lit(2)).otherwise(F.lit(3)).alias("TRASP_TIPO_ID"),
            F.col("FASCIA_KM").alias("TRASP_FASCIA_KM_COD"),
            F.col("NUMBOLLA").alias("NUM_BOLLA_SPED"),
            fn_clean_dat_d(F.col("DATABOLLA")).alias("GIORNO_BOLLA_SPED_ID"),
            F.lit(0).alias("AFFID_TEMPI_TRASP_COD"),
            F.col("STATO").alias("STATO_SPED_COD"),
            fn_clean_dat_d(F.col("DATACONSPREV")).alias("GIORNO_PREV_CONS_SOCIO_ID"),
            F.date_format(F.col("DATACONSPREV"), "HHmm").alias("ORA_PREV_CONS_SOCIO"),
            fn_clean_dat_d(F.col("DATA_AZIONE")).alias("GIORNO_CONS_SOCIO_ID"),
            F.date_format(F.col("DATA_AZIONE"), "HHmm").alias("ORA_CONS_SOCIO"),
            F.col("DATA_PARTENZA").alias("GIORNO_SPED_ID"),
            F.col("ORA_PARTENZA").alias("ORA_SPED"),
            F.col("MAG_SITO_PASSAGGIO_COD").alias("MAG_SITO_TRANSITO_COD"),
            # DECODE(NVL(MAG_SITO_PASSAGGIO_COD,0), 0, 0, 1)
            F.when(F.coalesce(F.col("MAG_SITO_PASSAGGIO_COD"), F.lit(0)) == 0, F.lit(0)).otherwise(F.lit(1)).alias("MAG_SITO_TRANSITO_FLAG"),
            F.coalesce(F.col("QTA"), F.lit(0)).alias("MTV_CONS_SOCIO"),
            F.coalesce(F.col("QTA_RP"), F.lit(0)).alias("MTV_RICEV_SOCIO"),
            F.coalesce(F.col("QTA_RT"), F.lit(0)).alias("MTV_RICEV_CEDI"),
            F.col("NUM_GITA"),
            F.col("NUOVA_GITA_FLAG"),
        )
    )

    # ── RAMO TRANSITO (TRANSITO_COD IS NOT NULL) → trasporto CEDI→CEDI ─────────
    transito = (
        src.filter(F.col("MAG_SITO_TRANSITO_COD").isNotNull())
        .select(
            fn_get_mag_sito_cod(F.col("MAG_SITO_COD"), F.col("DATA_AZIONE")).alias("MAG_SITO_COD"),
            F.col("MAG_SITO_COD").alias("TRASP_NODO_ORIG_COD"),
            F.col("MAG_SITO_TRANSITO_COD").alias("TRASP_NODO_DEST_COD"),
            F.col("AUTISTA_SPED_COD"),
            F.col("AUTOMEZZO").alias("AUTOM_SPED_COD"),
            F.col("VETTORE").alias("VETTORE_SPED_COD"),
            F.col("SP_ID"),
            F.col("MTV_COD"),
            F.lit(1).alias("TRASP_TIPO_ID"),  # 1 = Da CEDI a CEDI
            F.col("FASCIA_KM").alias("TRASP_FASCIA_KM_COD"),
            F.col("NUMBOLLA").alias("NUM_BOLLA_SPED"),
            fn_clean_dat_d(F.col("DATABOLLA")).alias("GIORNO_BOLLA_SPED_ID"),
            F.lit(0).alias("AFFID_TEMPI_TRASP_COD"),
            F.col("STATO").alias("STATO_SPED_COD"),
            fn_clean_dat_d(F.col("DATACONSPREV")).alias("GIORNO_PREV_CONS_SOCIO_ID"),
            F.date_format(F.col("DATACONSPREV"), "HHmm").alias("ORA_PREV_CONS_SOCIO"),
            fn_clean_dat_d(F.col("DATA_AZIONE")).alias("GIORNO_CONS_SOCIO_ID"),
            F.date_format(F.col("DATA_AZIONE"), "HHmm").alias("ORA_CONS_SOCIO"),
            F.col("DATA_PARTENZA").alias("GIORNO_SPED_ID"),
            F.col("ORA_PARTENZA").alias("ORA_SPED"),
            F.lit(None).cast("string").alias("MAG_SITO_TRANSITO_COD"),
            F.lit(0).alias("MAG_SITO_TRANSITO_FLAG"),
            F.coalesce(F.col("QTA"), F.lit(0)).alias("MTV_CONS_SOCIO"),
            F.coalesce(F.col("QTA_RP"), F.lit(0)).alias("MTV_RICEV_SOCIO"),
            F.coalesce(F.col("QTA_RT"), F.lit(0)).alias("MTV_RICEV_CEDI"),
            F.col("NUM_GITA"),
            F.col("NUOVA_GITA_FLAG"),
        )
    )

    silver_df = (
        cons.unionByName(transito)
        .withColumn("_silver_ts", F.current_timestamp())
        .withColumn("_silver_load_date", F.lit(run_date).cast("date"))
    )

    check_not_null(silver_df, ["SP_ID", "MAG_SITO_COD", "NUM_BOLLA_SPED"], NOTEBOOK_NAME)
    rows_clean = silver_df.count()
    logger.info(f"Righe silver (cons+transito): {rows_clean}")
    check_row_count(silver_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    MERGE_KEYS = ["SP_ID", "MAG_SITO_COD", "GIORNO_BOLLA_SPED_ID", "NUM_BOLLA_SPED"]

    if not spark.catalog.tableExists(TARGET_TABLE):
        logger.info(f"Prima esecuzione — CTAS {TARGET_TABLE}")
        silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)
    else:
        cond = " AND ".join([f"tgt.{k} = src.{k}" for k in MERGE_KEYS])
        DeltaTable.forName(spark, TARGET_TABLE).alias("tgt").merge(
            silver_df.alias("src"), cond
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.info(f"MERGE INTO {TARGET_TABLE} completato")

    logger.info(f"END {NOTEBOOK_NAME} | righe_lette={rows_read} | righe_silver={rows_clean}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
