# Databricks notebook source
# Area: Preparazione Spedizioni
# Layer: Silver
# Versione: 3.0.0
# Autore: Luigi Scrimitore
# Data: 2026-06-08
# Descrizione: Bolle Silver — produce DUE target separati a partire dalle sorgenti STAT (OP-16):
#   1) silver.logistica.bolla_testata   da bronze.logistica.testate_bolle (TEBO_*, 26 col)
#                                        chiave SITO + NRO_BOLLA + DATA_BOLLA
#   2) silver.logistica.bolla_dettaglio da bronze.logistica.storico_bolle (BOL_*, 87 col)
#                                        chiave SITO + NRO_BOLLA + DATA_BOLLA + NRO_RIGA
#   Sorgente STAT (OP-16): testate_bolle e storico_bolle NON provengono da Logistix ma dal
#   sistema STAT (path unico, NON multi-sito -> nessun _sito_cod nel Bronze).
#   Mapping prefisso TEBO_*/BOL_* -> business SOLO su colonne realmente presenti nei Bronze.
#   Filtro su _bronze_load_date = run_date, deduplica Window su _bronze_insert_ts DESC,
#   cast espliciti, _silver_ts, MERGE INTO su chiave naturale (CTAS prima esecuzione).

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog, julian_to_date, normalize_sito, get_sito_alias_map

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME  = "silver_prep_bolle"
BRONZE_CATALOG = get_catalog("bronze", env)
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SRC_TESTATE    = f"{BRONZE_CATALOG}.{SCHEMA}.testate_bolle"
SRC_STORICO    = f"{BRONZE_CATALOG}.{SCHEMA}.storico_bolle"
TGT_TESTATA    = f"{SILVER_CATALOG}.{SCHEMA}.bolla_testata"
TGT_DETTAGLIO  = f"{SILVER_CATALOG}.{SCHEMA}.bolla_dettaglio"

# COMMAND ----------

logger = get_logger(NOTEBOOK_NAME)

try:
    _amap = get_sito_alias_map(spark, f"{BRONZE_CATALOG}.{SCHEMA}")
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    # ─────────────────────────────────────────────────────────────────────────
    # PARTE 1 — bolla_testata da testate_bolle (TEBO_*)
    # ─────────────────────────────────────────────────────────────────────────
    testate_raw = (
        spark.table(SRC_TESTATE)
        .filter(F.col("_bronze_load_date") == run_date)
    )
    rows_testate = testate_raw.count()
    logger.info(f"Testate lette: {rows_testate}")

    check_not_null(testate_raw, ["TEBO_SITO", "TEBO_NRO_BOLLA", "TEBO_DATA_BOLLA"], NOTEBOOK_NAME)

    w_t = Window.partitionBy("TEBO_SITO", "TEBO_NRO_BOLLA", "TEBO_DATA_BOLLA") \
                .orderBy(F.col("_bronze_insert_ts").desc())

    testata_df = (
        testate_raw
        .withColumn("_rn", F.row_number().over(w_t))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        # Mapping TEBO_* -> business SOLO su colonne reali (26 col, tutto StringType)
        .select(
            normalize_sito(F.col("TEBO_SITO"), _amap).alias("SITO_COD"),
            F.col("TEBO_COD_MAGAZZINO").cast("string").alias("MAGAZZINO_COD"),
            F.col("TEBO_COD_NEGOZIO").cast("string").alias("NEGOZIO_COD"),
            F.col("TEBO_NRO_BOLLA").cast("string").alias("BOLLA_NRO"),
            julian_to_date(F.col("TEBO_DATA_BOLLA")).alias("DATA_BOLLA"),
            julian_to_date(F.col("TEBO_DATA_PARTENZA")).alias("DATA_PARTENZA"),
            F.col("TEBO_ORA_PARTENZA").cast("string").alias("ORA_PARTENZA"),
            julian_to_date(F.col("TEBO_DATA_CONSEGNA")).alias("DATA_CONSEGNA"),
            F.col("TEBO_ORA_CONSEGNA").cast("string").alias("ORA_CONSEGNA"),
            F.col("TEBO_COD_AUTISTA").cast("string").alias("AUTISTA_COD"),
            F.col("TEBO_COD_AUTOMEZZO").cast("string").alias("AUTOMEZZO_COD"),
            F.col("TEBO_COD_VETTORE").cast("string").alias("VETTORE_COD"),
            F.col("TEBO_FLAG_ADDEBITO").cast("string").alias("FLAG_ADDEBITO"),
            F.col("TEBO_MAG_TRANSITO").cast("string").alias("MAGAZZINO_TRANSITO"),
            F.col("TEBO_NRO_SIGILLO").cast("string").alias("SIGILLO_NRO"),
            F.col("TEBO_NRO_SIGILLO_RIT").cast("string").alias("SIGILLO_RIT_NRO"),
            F.col("TEBO_SPEDIZIONIERE").cast("string").alias("SPEDIZIONIERE_COD"),
            F.col("TEBO_SOPCODSOC_CDT").cast("string").alias("SOCIETA_CDT_COD"),
            F.col("TEBO_SOPSOCIO_FATTURAZIONE").cast("string").alias("SOCIO_FATTURAZIONE"),
            julian_to_date(F.col("TEBO_DATA_GENERAZIONE_BOLLA")).alias("DATA_GENERAZIONE_BOLLA"),
            F.col("TEBO_NOME_UTENTE").cast("string").alias("NOME_UTENTE"),
            F.col("_bronze_insert_ts"),
            F.col("_bronze_load_date"),
        )
        .withColumn("_silver_ts", F.current_timestamp())
    )

    rows_testata_clean = testata_df.count()
    logger.info(f"Testate silver dopo deduplica: {rows_testata_clean}")
    check_row_count(testata_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    if not spark.catalog.tableExists(TGT_TESTATA):
        logger.info(f"Prima esecuzione — CTAS {TGT_TESTATA}")
        testata_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TGT_TESTATA)
    else:
        DeltaTable.forName(spark, TGT_TESTATA).alias("tgt").merge(
            testata_df.alias("src"),
            "tgt.SITO_COD = src.SITO_COD "
            "AND tgt.BOLLA_NRO = src.BOLLA_NRO "
            "AND tgt.DATA_BOLLA = src.DATA_BOLLA"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    logger.info(f"bolla_testata MERGE completato | righe_silver={rows_testata_clean}")

    # ─────────────────────────────────────────────────────────────────────────
    # PARTE 2 — bolla_dettaglio da storico_bolle (BOL_*)
    # ─────────────────────────────────────────────────────────────────────────
    storico_raw = (
        spark.table(SRC_STORICO)
        .filter(F.col("_bronze_load_date") == run_date)
    )
    rows_storico = storico_raw.count()
    logger.info(f"Righe storico_bolle lette: {rows_storico}")

    check_not_null(storico_raw, ["BOL_SITO", "BOL_NRO_BOLLA", "BOL_DATA_BOLLA"], NOTEBOOK_NAME)

    w_b = Window.partitionBy("BOL_SITO", "BOL_NRO_BOLLA", "BOL_DATA_BOLLA", "BOL_NRO_RIGA") \
                .orderBy(F.col("_bronze_insert_ts").desc())

    dettaglio_df = (
        storico_raw
        .withColumn("_rn", F.row_number().over(w_b))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        # Mapping BOL_* -> business SOLO su colonne reali (87 col, tutto StringType)
        .select(
            normalize_sito(F.col("BOL_SITO"), _amap).alias("SITO_COD"),
            F.col("BOL_COD_MAGAZZINO").cast("string").alias("MAGAZZINO_COD"),
            F.col("BOL_COD_SETTOR_MAG").cast("string").alias("SETTORE_MAG_COD"),
            F.col("BOL_NRO_BOLLA").cast("string").alias("BOLLA_NRO"),
            julian_to_date(F.col("BOL_DATA_BOLLA")).alias("DATA_BOLLA"),
            F.col("BOL_NRO_RIGA").cast("int").alias("RIGA_NRO"),
            F.col("BOL_NRO_RIEPILOGO").cast("string").alias("RIEPILOGO_NRO"),
            F.col("BOL_TIPO_RIEPILOGO").cast("string").alias("TIPO_RIEPILOGO"),
            F.col("BOL_NRO_ORDINE").cast("string").alias("ORDINE_NRO"),
            F.col("BOL_NRO_ORDINE_NEG").cast("string").alias("ORDINE_NEGOZIO_NRO"),
            F.col("BOL_TIPO_ORDINE").cast("string").alias("TIPO_ORDINE"),
            julian_to_date(F.col("BOL_DATA_ORDIN_NEG")).alias("DATA_ORDINE_NEGOZIO"),
            F.col("BOL_QTA_ORDINE_NEG").cast("decimal(14,3)").alias("QTA_ORDINE_NEGOZIO"),
            F.col("BOL_COD_NEGOZIO").cast("string").alias("NEGOZIO_COD"),
            F.col("BOL_COD_REPARTO").cast("string").alias("REPARTO_COD"),
            F.col("BOL_COD_REPAR_PREP").cast("string").alias("REPARTO_PREP_COD"),
            F.col("BOL_AREA_NEGOZIO").cast("string").alias("AREA_NEGOZIO"),
            F.col("BOL_COD_GESTIONE").cast("string").alias("GESTIONE_COD"),
            F.col("BOL_TIPO_MOVIMENTO").cast("string").alias("TIPO_MOVIMENTO"),
            F.col("BOL_COD_MSI").cast("string").alias("MSI_COD"),
            F.col("BOL_COD_EAN").cast("string").alias("EAN_COD"),
            F.col("BOL_QTA_EVASA").cast("decimal(14,3)").alias("QTA_EVASA"),
            F.col("BOL_QTA_DA_EVADERE").cast("decimal(14,3)").alias("QTA_DA_EVADERE"),
            F.col("BOL_QTA_UF_SPED").cast("decimal(14,3)").alias("QTA_UF_SPEDITA"),
            F.col("BOL_PZ_CARTONE").cast("decimal(14,3)").alias("PEZZI_CARTONE"),
            F.col("BOL_CART_X_STRATO").cast("decimal(14,3)").alias("CARTONI_X_STRATO"),
            F.col("BOL_STRATI_PALLET").cast("decimal(14,3)").alias("STRATI_PALLET"),
            F.col("BOL_CART_X_ULT_ST").cast("decimal(14,3)").alias("CARTONI_X_ULT_STRATO"),
            F.col("BOL_TIPO_PRELIEVO").cast("string").alias("TIPO_PRELIEVO"),
            F.col("BOL_SEQUE_PRELIEVO").cast("string").alias("SEQUENZA_PRELIEVO"),
            F.col("BOL_COD_CARRELLIST").cast("string").alias("CARRELLISTA_COD"),
            F.col("BOL_NRO_GABBIA").cast("string").alias("GABBIA_NRO"),
            F.col("BOL_NRO_ETICHETTA").cast("string").alias("ETICHETTA_NRO"),
            F.col("BOL_NRO_CARICO").cast("string").alias("CARICO_NRO"),
            julian_to_date(F.col("BOL_DATA_SCADENZA")).alias("DATA_SCADENZA"),
            F.col("BOL_COD_SOSTITUTO").cast("string").alias("SOSTITUTO_COD"),
            F.col("BOL_FLAG_SCARTATO").cast("string").alias("FLAG_SCARTATO"),
            F.col("BOL_IVA").cast("string").alias("IVA"),
            F.col("BOL_PERC_IVA").cast("decimal(7,4)").alias("PERC_IVA"),
            F.col("BOL_COD_CLASS_FISC").cast("string").alias("CLASSE_FISCALE_COD"),
            F.col("BOL_PRZ_ACQ_NETTO").cast("decimal(18,4)").alias("PREZZO_ACQ_NETTO"),
            F.col("BOL_PRZ_ACQ_MEDIO").cast("decimal(18,4)").alias("PREZZO_ACQ_MEDIO"),
            F.col("BOL_PRZ_ACQUISTO").cast("decimal(18,4)").alias("PREZZO_ACQUISTO"),
            F.col("BOL_PRZ_CESSIONE").cast("decimal(18,4)").alias("PREZZO_CESSIONE"),
            F.col("BOL_PRZ_VENDITA").cast("decimal(18,4)").alias("PREZZO_VENDITA"),
            F.col("BOL_CALO_ADDEBITO").cast("decimal(14,3)").alias("CALO_ADDEBITO"),
            F.col("BOL_CALO_VENDITA").cast("decimal(14,3)").alias("CALO_VENDITA"),
            julian_to_date(F.col("BOL_DATA_PARTENZA")).alias("DATA_PARTENZA"),
            F.col("BOL_ORA_PARTENZA").cast("string").alias("ORA_PARTENZA"),
            julian_to_date(F.col("BOL_DATA_CONSEGNA")).alias("DATA_CONSEGNA"),
            F.col("BOL_ORA_CONSEGNA").cast("string").alias("ORA_CONSEGNA"),
            F.col("BOL_COD_AUTISTA").cast("string").alias("AUTISTA_COD"),
            F.col("BOL_COD_AUTOMEZZO").cast("string").alias("AUTOMEZZO_COD"),
            F.col("BOL_COD_VETTORE").cast("string").alias("VETTORE_COD"),
            F.col("BOL_SPEDIZIONIERE").cast("string").alias("SPEDIZIONIERE_COD"),
            julian_to_date(F.col("BOL_DATA_PREPARAZ")).alias("DATA_PREPARAZ"),
            julian_to_date(F.col("BOL_DATA_FINE_PREP")).alias("DATA_FINE_PREP"),
            F.col("BOL_ORA_FINE_PREP").cast("string").alias("ORA_FINE_PREP"),
            F.col("BOL_MODALITA_PESATA").cast("string").alias("MODALITA_PESATA"),
            F.col("BOL_VALUTA_PREZZI").cast("string").alias("VALUTA_PREZZI"),
            F.col("BOL_CATENA").cast("string").alias("CATENA"),
            F.col("BOL_COD_MSI_CATENA").cast("string").alias("MSI_CATENA_COD"),
            F.col("BOL_NRO_CONSEGNA").cast("string").alias("CONSEGNA_NRO"),
            F.col("BOL_NRO_RIGA_NEG").cast("string").alias("RIGA_NEGOZIO_NRO"),
            F.col("BOL_NOME_UTENTE").cast("string").alias("NOME_UTENTE"),
            julian_to_date(F.col("BOL_DATA_GENERAZIONE_BOLLA")).alias("DATA_GENERAZIONE_BOLLA"),
            F.col("_bronze_insert_ts"),
            F.col("_bronze_load_date"),
        )
        .withColumn("_silver_ts", F.current_timestamp())
    )

    rows_dettaglio_clean = dettaglio_df.count()
    logger.info(f"Dettaglio silver dopo deduplica: {rows_dettaglio_clean}")
    check_row_count(dettaglio_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    if not spark.catalog.tableExists(TGT_DETTAGLIO):
        logger.info(f"Prima esecuzione — CTAS {TGT_DETTAGLIO}")
        dettaglio_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TGT_DETTAGLIO)
    else:
        DeltaTable.forName(spark, TGT_DETTAGLIO).alias("tgt").merge(
            dettaglio_df.alias("src"),
            "tgt.SITO_COD = src.SITO_COD "
            "AND tgt.BOLLA_NRO = src.BOLLA_NRO "
            "AND tgt.DATA_BOLLA = src.DATA_BOLLA "
            "AND tgt.RIGA_NRO = src.RIGA_NRO"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    logger.info(f"bolla_dettaglio MERGE completato | righe_silver={rows_dettaglio_clean}")
    logger.info(
        f"END {NOTEBOOK_NAME} | testate={rows_testata_clean} | dettagli={rows_dettaglio_clean}"
    )

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
