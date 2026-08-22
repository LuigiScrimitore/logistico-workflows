# Databricks notebook source
# Area: Carichi
# Layer: Silver
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: Dettagli Carichi Silver — sorgente bronze.logistica.sto_righe_carico (prefisso SRCAR_).
#              Filtra il Bronze per _bronze_load_date = run_date (delta del giorno), rinomina le
#              colonne Oracle reali (verificate da SOURCE_COLS del Bronze), cast espliciti,
#              deduplica su chiave naturale (Window su _bronze_insert_ts DESC),
#              MERGE INTO silver.logistica.carico_dettaglio (CTAS la prima volta).
#              OP-12: la normalizzazione articolo radice/variante e' applicata QUI in Silver a partire
#              da SRCAR_COD_MSI (codice articolo logistico) tramite la funzione normalize_articolo
#              (placeholder: la regola di splitting radice/variante va confermata con Reply).
#              NB: nessun ammanco per-riga (concetto di ordine/gruppo); vive in gold A_INBOUND (pezzi).
#              NOTA: il Bronze NON contiene QTA_RICEVUTA/QTA_STORNATA/COD_STATO: colonne inventate rimosse.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog, julian_to_date, normalize_sito, get_sito_alias_map, art_radice, art_variante, read_watermark, update_watermark

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"], "Full refresh")
dbutils.widgets.text("process_from", "", "Process from (override watermark)")

env                  = dbutils.widgets.get("env")
run_date             = dbutils.widgets.get("run_date")
full_refresh         = dbutils.widgets.get("full_refresh") == "true"
process_from_widget  = dbutils.widgets.get("process_from").strip()

# COMMAND ----------

NOTEBOOK_NAME  = "silver_carichi_dettagli"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{BRONZE_CATALOG}.{SCHEMA}.sto_righe_carico"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.carico_dettaglio"

WM_STAGE, WM_SISTEMA, WM_TABELLA = "bronze_to_clean", "logistix", "sto_righe_carico"

# COMMAND ----------
# MAGIC %md #### OP-12 — Normalizzazione articolo radice/variante (placeholder)

# COMMAND ----------

def normalize_articolo(df, source_col="ART_COD"):
    """OP-12: deriva ART_RADICE e ART_VAR dal codice articolo logistico (SRCAR_COD_MSI → ART_COD).

    Regola CONFERMATA (=FN_GET_RADICE/FN_GET_VARIANTE_LOGISTICA per LOGISTIX/SWAP/STAT):
      - il codice MSI e' composito = <codice radice> + <variante a 3 cifre>
      - ART_RADICE = MSI senza le ultime 3 cifre  (es. 2534106004 -> 2534106)
      - ART_VAR    = ultime 3 cifre               (es. 2534106004 -> 004)
    Cosi' ART_RADICE aggancia direttamente LU_ART_RADICE.ART_RADICE_COD (Retail/CDT_DW).
    Caso limite (codice <= 3 cifre): radice = codice intero, variante = NULL.

    DRY: usa le utility centralizzate art_radice/art_variante di logistica_utils
    (stessa logica del placeholder locale precedente; allineamento sicuro).
    """
    return (
        df
        .withColumn("ART_RADICE", art_radice(F.col(source_col)))
        .withColumn("ART_VAR",    art_variante(F.col(source_col)))
    )

# COMMAND ----------

logger = get_logger(NOTEBOOK_NAME)

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    raw_df = spark.table(SOURCE_TABLE)
    incremental = (not full_refresh) and spark.catalog.tableExists(TARGET_TABLE)
    process_from = None
    if incremental:
        process_from = process_from_widget or read_watermark(spark, env, WM_STAGE, WM_SISTEMA, WM_TABELLA)
        if process_from:
            raw_df = raw_df.filter(F.col("_bronze_load_date") > F.lit(str(process_from)).cast("date"))
    rows_read = raw_df.count()
    logger.info(f"Righe lette da {SOURCE_TABLE} ({'INCREMENTALE >'+str(process_from) if incremental and process_from else 'FULL'}): {rows_read}")

    check_not_null(raw_df, ["MAG_SITO_COD", "SRCAR_NRO_CARICO", "SRCAR_COD_MSI", "SRCAR_COD_MAGAZZINO"], NOTEBOOK_NAME)
    check_row_count(raw_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    _amap = get_sito_alias_map(spark, f"{BRONZE_CATALOG}.{SCHEMA}")

    # ── Rinomina colonne Oracle reali → nomi business (solo colonne presenti in SOURCE_COLS) ──
    renamed_df = (
        raw_df
        .withColumn("MAG_SITO_COD", normalize_sito(F.col("MAG_SITO_COD"), _amap))
        .withColumnRenamed("MAG_SITO_COD",        "SITO_COD")
        .withColumnRenamed("SRCAR_NRO_CARICO",    "CARICO_NRO")
        .withColumnRenamed("SRCAR_COD_MSI",       "MSI_COD")
        .withColumnRenamed("SRCAR_COD_MAGAZZINO", "MAG_COD")
        .withColumnRenamed("SRCAR_COD_SEDE",      "SEDE_COD")
        .withColumnRenamed("SRCAR_NRO_ORDINE",    "ORDINE_NRO")
        .withColumnRenamed("SRCAR_COD_EAN",       "EAN_COD")
        .withColumnRenamed("SRCAR_DES_PRODOTTO",  "ART_DESC")
        .withColumnRenamed("SRCAR_UDM_ACQUISTI",  "UDM_ACQUISTI")
        .withColumnRenamed("SRCAR_QTA_ORDINATA",  "QTA_ORDINATA")
        .withColumnRenamed("SRCAR_NRO_PZ_CARICAT","NRO_PZ_CARICATI")
        .withColumnRenamed("SRCAR_QTA_UF_RIL",    "QTA_UF_RILEVATA")
        .withColumnRenamed("SRCAR_QTA_PZ_FOR",    "QTA_PZ_FORNITORE")
        .withColumnRenamed("SRCAR_QTA_UF_FOR",    "QTA_UF_FORNITORE")
        .withColumnRenamed("SRCAR_PESO_CARTONE",  "PESO_CARTONE")
        .withColumnRenamed("SRCAR_PRZ_ACQUISTO",  "PREZZO_ACQUISTO")
        .withColumnRenamed("SRCAR_PRZ_CESSIONE",  "PREZZO_CESSIONE")
        .withColumnRenamed("SRCAR_COD_RICEVITORE","RICEVITORE_COD")
        .withColumnRenamed("SRCAR_NRO_BOLLA_FORN","BOLLA_FORN_NRO")
        .withColumnRenamed("SRCAR_DATA_BOLLA_FOR","DATA_BOLLA_FORN")
        .withColumnRenamed("SRCAR_DATA_CARICO",   "DATA_CARICO")
        .withColumnRenamed("SRCAR_ORA_CARICO",    "ORA_CARICO")
        .withColumnRenamed("SRCAR_DATA_SCADENZA", "DATA_SCADENZA")
        .withColumnRenamed("SRCAR_DATA_MODIFICA", "DATA_MODIFICA")
        .withColumnRenamed("SRCAR_FLAG_SOSPESO",  "FLG_SOSPESO_RAW")
        .withColumnRenamed("SRCAR_TIPO_DOCUMENTO","TIPO_DOCUMENTO")
        .withColumnRenamed("SRCAR_TIPOMOV",       "TIPO_MOVIMENTO")
        .withColumnRenamed("SRCAR_NOTE_RICEVIM",  "NOTE_RICEVIMENTO")
        .withColumnRenamed("SRCAR_NOTE_COMMERCIA","NOTE_COMMERCIALE")
        # ── Struttura imballo/pallet (nomi allineati a CDT_DW.F_CARICO per quadratura) ──
        .withColumnRenamed("SRCAR_PZ_CARTONE",     "NUM_PZ_IMB_SITO")        # pezzi per imballo (sito)
        .withColumnRenamed("SRCAR_PZ_CART_FOR",    "NUM_PZ_IMB_EFF_FORN")    # pezzi per imballo (fornitore)
        .withColumnRenamed("SRCAR_PZ_CARTONE_ORD", "NUM_PZ_IMB_ORD_FORN")    # pezzi per imballo (ordinato)
        .withColumnRenamed("SRCAR_CART_X_STRATO",  "NUM_IMB_STRATO_PLT_SITO")# imballi per strato pallet (sito)
        .withColumnRenamed("SRCAR_STRATI_PALLET",  "NUM_STRATO_PLT_SITO")    # strati per pallet (sito)
        .withColumnRenamed("SRCAR_CART_X_ULT_ST",  "NUM_IMB_ULT_STRATO_SITO")# imballi ultimo strato (sito)
        .withColumnRenamed("SRCAR_CART_X_ST_FOR",  "NUM_IMB_STRATO_PLT_FORN")# imballi per strato pallet (fornitore)
        .withColumnRenamed("SRCAR_STRATI_PA_FOR",  "NUM_STRATO_PLT_FORN")    # strati per pallet (fornitore)
        .withColumnRenamed("SRCAR_CART_X_UL_FOR",  "NUM_IMB_ULT_STRATO_FORN")# imballi ultimo strato (fornitore)
    )
    # ART_COD business = codice articolo logistico (MSI). Manteniamo anche MSI_COD come chiave.
    renamed_df = renamed_df.withColumn("ART_COD", F.col("MSI_COD"))

    # ── Deduplica: ultima versione per chiave naturale ────────────────────────
    w = Window.partitionBy("SITO_COD", "CARICO_NRO", "MSI_COD", "MAG_COD").orderBy(F.col("_bronze_insert_ts").desc())
    deduped_df = (
        renamed_df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ── OP-12: normalizzazione articolo radice/variante (in Silver) ───────────
    deduped_df = normalize_articolo(deduped_df, source_col="ART_COD")

    # ── Cast tipi (Bronze è tutto StringType) ─────────────────────────────────
    silver_df = (
        deduped_df
        # Date Julian Day legacy (NUMBER) -> date calendario
        .withColumn("DATA_CARICO",     julian_to_date(F.col("DATA_CARICO")))
        .withColumn("DATA_BOLLA_FORN", julian_to_date(F.col("DATA_BOLLA_FORN")))
        .withColumn("DATA_SCADENZA",   julian_to_date(F.col("DATA_SCADENZA")))
        .withColumn("DATA_MODIFICA",   julian_to_date(F.col("DATA_MODIFICA")))
        .withColumn("QTA_ORDINATA",     F.col("QTA_ORDINATA").cast("decimal(14,3)"))
        .withColumn("NRO_PZ_CARICATI",  F.col("NRO_PZ_CARICATI").cast("decimal(14,3)"))
        .withColumn("QTA_UF_RILEVATA",  F.col("QTA_UF_RILEVATA").cast("decimal(14,3)"))
        .withColumn("QTA_PZ_FORNITORE", F.col("QTA_PZ_FORNITORE").cast("decimal(14,3)"))
        .withColumn("QTA_UF_FORNITORE", F.col("QTA_UF_FORNITORE").cast("decimal(14,3)"))
        .withColumn("PESO_CARTONE",     F.col("PESO_CARTONE").cast("decimal(12,3)"))
        .withColumn("PREZZO_ACQUISTO",  F.col("PREZZO_ACQUISTO").cast("decimal(16,4)"))
        .withColumn("PREZZO_CESSIONE",  F.col("PREZZO_CESSIONE").cast("decimal(16,4)"))
        # ── Struttura imballo/pallet: conteggi -> decimal(14,3) (robusto a valori con decimali) ──
        .withColumn("NUM_PZ_IMB_SITO",         F.col("NUM_PZ_IMB_SITO").cast("decimal(14,3)"))
        .withColumn("NUM_PZ_IMB_EFF_FORN",     F.col("NUM_PZ_IMB_EFF_FORN").cast("decimal(14,3)"))
        .withColumn("NUM_PZ_IMB_ORD_FORN",     F.col("NUM_PZ_IMB_ORD_FORN").cast("decimal(14,3)"))
        .withColumn("NUM_IMB_STRATO_PLT_SITO", F.col("NUM_IMB_STRATO_PLT_SITO").cast("decimal(14,3)"))
        .withColumn("NUM_STRATO_PLT_SITO",     F.col("NUM_STRATO_PLT_SITO").cast("decimal(14,3)"))
        .withColumn("NUM_IMB_ULT_STRATO_SITO", F.col("NUM_IMB_ULT_STRATO_SITO").cast("decimal(14,3)"))
        .withColumn("NUM_IMB_STRATO_PLT_FORN", F.col("NUM_IMB_STRATO_PLT_FORN").cast("decimal(14,3)"))
        .withColumn("NUM_STRATO_PLT_FORN",     F.col("NUM_STRATO_PLT_FORN").cast("decimal(14,3)"))
        .withColumn("NUM_IMB_ULT_STRATO_FORN", F.col("NUM_IMB_ULT_STRATO_FORN").cast("decimal(14,3)"))
        # ── Conversione boolean SRCAR_FLAG_SOSPESO: 'S' → true ───────────────
        .withColumn(
            "FLG_SOSPESO",
            F.upper(F.trim(F.col("FLG_SOSPESO_RAW"))) == F.lit("S")
        )
        .drop("FLG_SOSPESO_RAW")
        # NB: nessun ammanco a livello riga/etichetta. L'ammanco (pezzi ordinati − ricevuti) è
        #     un concetto di ordine/gruppo e vive SOLO in gold A_INBOUND (unità pezzi, coerente).
        #     QTA_ORDINATA qui resta in COLLI (grezzo sorgente); la conversione a pezzi è a valle.
        .withColumn("_silver_ts", F.current_timestamp())
    )

    rows_clean = silver_df.count()
    logger.info(f"Righe dopo deduplica: {rows_clean}")

    # ── MERGE INTO Silver (CTAS la prima volta) ───────────────────────────────
    if not spark.catalog.tableExists(TARGET_TABLE):
        logger.info(f"Creazione iniziale tabella {TARGET_TABLE}")
        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(TARGET_TABLE)
        )
    else:
        delta_target = DeltaTable.forName(spark, TARGET_TABLE)
        (
            delta_target.alias("tgt")
            .merge(
                silver_df.alias("src"),
                (
                    "tgt.SITO_COD = src.SITO_COD AND "
                    "tgt.CARICO_NRO = src.CARICO_NRO AND "
                    "tgt.MSI_COD = src.MSI_COD AND "
                    "tgt.MAG_COD = src.MAG_COD"
                )
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    logger.info(f"END {NOTEBOOK_NAME} | righe_lette={rows_read} | righe_silver={rows_clean}")

    new_wm = silver_df.agg(F.max("_bronze_load_date")).collect()[0][0]
    if new_wm is not None:
        update_watermark(spark, env, WM_STAGE, WM_SISTEMA, WM_TABELLA,
                         last_processed_date=new_wm, rows_processed=rows_clean, esito="OK")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    try:
        update_watermark(spark, env, WM_STAGE, WM_SISTEMA, WM_TABELLA,
                         esito="FAIL", message=str(e)[:500])
    except Exception:
        pass
    raise
