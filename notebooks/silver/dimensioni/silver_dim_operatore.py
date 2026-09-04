# Databricks notebook source
# Area: Dimensioni
# Layer: Silver
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: Dimensione Operatore — UNION delle 4 anagrafiche operatore logistix (OP-15):
#              bronze.logistica.carrellisti, .preparatori, .ricevitori, .spedizionieri.
#              Colonne reali verificate sui Bronze sorgente:
#                - carrellisti : CRLLS_COD_CARRELLIST / CRLLS_DES_CARRELLIST / CRLLS_FLAG_CARR_ATT
#                - preparatori : PREP_COD_PREPARATOR / PREP_DES_PREPARATOR / PREP_FLAG_PREP_ATT
#                - ricevitori  : RICV_COD_RICEVITOR / RICV_COGNOME+RICV_NOME / RICV_FLAG_RICV_ATT
#                - spedizionieri: SPE_CODICE / SPE_COGNOME+SPE_NOME / FLG_ATTIVO assente (null)
#              SITO_COD = MAG_SITO_COD per tutte. NESSUN attributo cognome/nome/contratto/data_assunzione
#              inventato per carrellisti/preparatori (usano la descrizione *_DES_*). Anagrafica FULL
#              -> overwrite completo. Dedup su (OPERATORE_COD, SITO_COD, TIPO_OPERATORE).

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog, normalize_sito, get_sito_alias_map

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from functools import reduce
from datetime import date

# COMMAND ----------
# MAGIC %md #### 1. Widget standard

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------
# MAGIC %md #### 2. Parametri notebook

# COMMAND ----------

NOTEBOOK_NAME  = "silver_dim_operatore"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"

SRC_CARRELLISTI   = f"{BRONZE_CATALOG}.{SCHEMA}.carrellisti"
SRC_PREPARATORI   = f"{BRONZE_CATALOG}.{SCHEMA}.preparatori"
SRC_RICEVITORI    = f"{BRONZE_CATALOG}.{SCHEMA}.ricevitori"
SRC_SPEDIZIONIERI = f"{BRONZE_CATALOG}.{SCHEMA}.spedizionieri"

TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.dim_operatore"
DEDUP_KEYS     = ["OPERATORE_COD", "SITO_COD", "TIPO_OPERATORE"]

logger = get_logger(NOTEBOOK_NAME)

TARGET_SCHEMA_COLS = [
    "OPERATORE_COD", "SITO_COD", "TIPO_OPERATORE",
    "DESCRIZIONE", "FLG_ATTIVO", "_bronze_insert_ts",
]

# COMMAND ----------
# MAGIC %md #### 3. Normalizzazione delle 4 anagrafiche su schema target unico

# COMMAND ----------

def _norm(df, cod_col, desc_expr, attivo_expr, tipo):
    return df.select(
        F.col(cod_col).cast("string").alias("OPERATORE_COD"),
        normalize_sito(F.col("MAG_SITO_COD"), _amap).alias("SITO_COD"),
        F.lit(tipo).alias("TIPO_OPERATORE"),
        desc_expr.cast("string").alias("DESCRIZIONE"),
        attivo_expr.cast("string").alias("FLG_ATTIVO"),
        F.col("_bronze_insert_ts"),
    )

try:
    _amap = get_sito_alias_map(spark, f"{BRONZE_CATALOG}.{SCHEMA}")
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    df_carr = _norm(
        spark.table(SRC_CARRELLISTI),
        "CRLLS_COD_CARRELLIST",
        F.col("CRLLS_DES_CARRELLIST"),
        F.col("CRLLS_FLAG_CARR_ATT"),
        "CARRELLISTA",
    )
    df_prep = _norm(
        spark.table(SRC_PREPARATORI),
        "PREP_COD_PREPARATOR",
        F.col("PREP_DES_PREPARATOR"),
        F.col("PREP_FLAG_PREP_ATT"),
        "PREPARATORE",
    )
    df_ricv = _norm(
        spark.table(SRC_RICEVITORI),
        "RICV_COD_RICEVITOR",
        F.trim(F.concat_ws(" ", F.col("RICV_COGNOME"), F.col("RICV_NOME"))),
        F.col("RICV_FLAG_RICV_ATT"),
        "RICEVITORE",
    )
    df_sped = _norm(
        spark.table(SRC_SPEDIZIONIERI),
        "SPE_CODICE",
        F.trim(F.concat_ws(" ", F.col("SPE_COGNOME"), F.col("SPE_NOME"))),
        F.lit(None),  # FLG_ATTIVO assente nel Bronze spedizionieri
        "SPEDIZIONIERE",
    )

    union_df = df_carr.unionByName(df_prep).unionByName(df_ricv).unionByName(df_sped)

    rows_read = union_df.count()
    logger.info(f"Righe lette (union 4 anagrafiche): {rows_read}")

    check_not_null(union_df, ["OPERATORE_COD", "SITO_COD", "TIPO_OPERATORE"], NOTEBOOK_NAME)



    w = Window.partitionBy(*DEDUP_KEYS).orderBy(F.col("_bronze_insert_ts").desc())

    silver_df = (
        union_df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_bronze_insert_ts")
        .withColumn("_silver_ts", F.current_timestamp())
        .select(
            "OPERATORE_COD", "SITO_COD", "TIPO_OPERATORE",
            "DESCRIZIONE", "FLG_ATTIVO", "_silver_ts",
        )
    )



    # OP-28: gli operatori visti nell'attivita' ma NON presenti nelle 4 anagrafiche master
    # finivano orfani (sentinella -1) nei fatti (F_PREP_SPED, F_TURNO_PREP_SITO, F_CARICO).
    # L'AS-IS si auto-curava: le procedure SP_AGG_ANAG_PREP_SPED3A/4A inserivano i codici FK
    # mancanti (errori 270607/270608) con descrizione 'Non definito'. Qui replichiamo il
    # comportamento recuperando i codici operatore da TUTTE le sorgenti d'attivita' (domini
    # distinti: prelievo=storico_liste, riepiloghi/turno=storico_riepiloghi, carico=sto_tes_carichi)
    # e aggiungendoli come TIPO_OPERATORE='NON_DEFINITO'.
    # (tabella, colonna_codice_operatore, colonna_sito)
    RECOVERY_SOURCES = [
        (f"{BRONZE_CATALOG}.{SCHEMA}.storico_liste",      "LSPRL_COD_PREPARATOR", "LSPRL_SITO"),
        (f"{BRONZE_CATALOG}.{SCHEMA}.storico_riepiloghi", "RPLPR_COD_PREPARATOR", "RPLPR_SITO"),
        (f"{BRONZE_CATALOG}.{SCHEMA}.sto_tes_carichi",    "STCAR_COD_OPERATORE",  "STCAR_COD_MAGAZZINO"),
    ]
    master_codes = silver_df.select("OPERATORE_COD").distinct()
    recovery_parts = []
    for tbl, cod_col, sito_col in RECOVERY_SOURCES:
        if not spark.catalog.tableExists(tbl):
            logger.warning(f"{tbl} assente: recovery operatori saltato per questa sorgente")
            continue
        src_tbl = spark.table(tbl)
        if cod_col not in src_tbl.columns or sito_col not in src_tbl.columns:
            logger.warning(f"{tbl}: colonne {cod_col}/{sito_col} assenti, skip")
            continue
        recovery_parts.append(
            src_tbl.select(
                F.col(cod_col).cast("string").alias("OPERATORE_COD"),
                normalize_sito(F.col(sito_col), _amap).alias("SITO_COD"),
            )
            .filter(F.col("OPERATORE_COD").isNotNull() & (F.trim(F.col("OPERATORE_COD")) != ""))
            .distinct()
        )

    if recovery_parts:
        rec_all = reduce(lambda a, b: a.unionByName(b), recovery_parts).distinct()
        recovery_df = (
            rec_all
            .join(master_codes, "OPERATORE_COD", "left_anti")  # solo codici assenti dal master
            .withColumn("TIPO_OPERATORE", F.lit("NON_DEFINITO"))
            .withColumn("DESCRIZIONE", F.lit("Non definito"))
            .withColumn("FLG_ATTIVO", F.lit(None).cast("string"))
            .withColumn("_silver_ts", F.current_timestamp())
            .select("OPERATORE_COD", "SITO_COD", "TIPO_OPERATORE",
                    "DESCRIZIONE", "FLG_ATTIVO", "_silver_ts")
            .dropDuplicates(["OPERATORE_COD", "SITO_COD"])
        )
        n_rec = recovery_df.count()
        logger.info(f"Self-healing operatori da attivita (liste+riepiloghi+carichi): +{n_rec} righe NON_DEFINITO")
        silver_df = silver_df.unionByName(recovery_df)
    else:
        logger.warning("Nessuna sorgente attivita disponibile: self-healing operatori saltato")

    # Membro esplicito "Non rilevato" (codice 'ND'): i fatti mappano l'operatore NULL/vuoto
    # (carico/riepilogo senza operatore registrato) a 'ND', agganciando questo membro reale,
    # cosi' i NULL legittimi non risultano orfani. Il sentinella '-1' resta riservato ai
    # codici davvero assenti dall'anagrafica.
    nd_member = spark.createDataFrame(
        [("ND", "ND", "NON_RILEVATO", "Non rilevato", None)],
        "OPERATORE_COD string, SITO_COD string, TIPO_OPERATORE string, "
        "DESCRIZIONE string, FLG_ATTIVO string",
    ).withColumn("_silver_ts", F.current_timestamp())
    silver_df = silver_df.unionByName(nd_member)

    rows_clean = silver_df.count()
    logger.info(f"Righe dopo dedup + self-healing + membro ND: {rows_clean}")
    check_row_count(silver_df, min_rows=1, notebook_name=NOTEBOOK_NAME)



    (silver_df.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(TARGET_TABLE))

    logger.info(f"END {NOTEBOOK_NAME} | righe_lette={rows_read} | righe_silver={rows_clean}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
