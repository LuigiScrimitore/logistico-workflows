# Databricks notebook source
# Area: CDT_ESTR (migrazione TO-BE) -> Giacenze / Stock
# Layer: Silver (elaborazione / target T_STOCK)
# Versione: 2.0.0
# Data: 2026-06-09
# Descrizione: Replica logica di V_STOCK_PICKING + V_STOCK_SCORTE + SP_INS_T_STOCK.
#              RIFATTO (v2.0): legge dalle SORGENTI RAW ricostruite, NON piu' dallo staging wl2_catena.
#              SORGENTI:
#                silver.logistica.catena_unificata   (catena_clean UNION catena_esterni_clean, dedup ST15)
#                silver.logistica.cndstostock_clean   (valori stock, arricchimento ST16 — opzionale)
#                struttura_mag                        (silver se presente, fallback bronze; mappa locazioni)
#              MODE: SNAPSHOT (append partizionato per data rilevazione).
#              LOGICA DI BUSINESS (resta qui, NON nei cleansing):
#                - discriminante STRM_FLAG_SERVIZIO='SI' (picking, prelievo) / !='SI' (scorte) — ST13
#                - filtro STRM_TIPO_STRUTTURA=0 (struttura standard) — ST13
#                - aggregato struttura per locazione: hxlxp (capienza pallet) + numrecs — per locz_tipo
#                - UNION ALL picking + scorte: predicati mutuamente esclusivi, no doppio conteggio — ST14
#              NB: la catena e' gia' pulita a monte (sito canonico, date DateType, ART_RADICE/VAR derivate):
#                  qui NON si ripete cleansing ne' mapping sito via tabgen.
#              Riferimento: CDT_ESTR_VISTE.sql 3015/3080; Revisione §6.4, §9-bis ST13/14/16.
#
# PUNTI APERTI (vedi report): fn_get_mappa_locz_tipo_cod (MAPPA_TIPO_LOCZ_COD=0 placeholder);
#   chiave di join catena<->cndstostock per i valori; ART_MODL_PES_COD (ST16, lookup G5).

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog, clean_dat_d, normalize_sito, get_sito_alias_map

from pyspark.sql import functions as F
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME    = "silver_t_stock"
BRONZE_CATALOG   = get_catalog("bronze", env)
SILVER_CATALOG   = get_catalog("silver", env)
SCHEMA           = "logistica"
SOURCE_CATENA    = f"{SILVER_CATALOG}.{SCHEMA}.catena_unificata"     # catena+esterni unificata e pulita
SOURCE_CNDSTOCK  = f"{SILVER_CATALOG}.{SCHEMA}.cndstostock_clean"    # valori stock (ST16)
STRUTTURA_SILVER = f"{SILVER_CATALOG}.{SCHEMA}.struttura_mag"        # se prodotta a valle
STRUTTURA_BRONZE = f"{BRONZE_CATALOG}.{SCHEMA}.struttura_mag"        # fallback (anagrafica FULL)
TARGET_TABLE     = f"{SILVER_CATALOG}.{SCHEMA}.t_stock"

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    if not spark.catalog.tableExists(SOURCE_CATENA):
        logger.warning(f"Sorgente {SOURCE_CATENA} non esiste. Notebook terminato.")
        dbutils.notebook.exit("NO_SOURCE")

    # struttura_mag: preferisci la silver se esiste, altrimenti la bronze (anagrafica FULL)
    if spark.catalog.tableExists(STRUTTURA_SILVER):
        struttura_table = STRUTTURA_SILVER
    elif spark.catalog.tableExists(STRUTTURA_BRONZE):
        struttura_table = STRUTTURA_BRONZE
    else:
        logger.warning("struttura_mag non disponibile (ne' silver ne' bronze). Notebook terminato.")
        dbutils.notebook.exit("NO_SOURCE")
    logger.info(f"struttura_mag sorgente: {struttura_table}")

    # ── Catena gia' pulita e unificata (sito canonico, date DateType, radice/variante) ──
    ca = spark.table(SOURCE_CATENA).filter(F.col("_bronze_load_date") == F.lit(run_date))
    rows_catena = ca.count()
    logger.info(f"Righe catena unificata (run_date={run_date}): {rows_catena}")
    if rows_catena == 0:
        logger.warning("Nessun dato giacenze. Notebook terminato.")
        dbutils.notebook.exit("NO_DATA")

    sm = spark.table(struttura_table)
    # FIX OP-29: struttura_mag.MAG_SITO_COD e' alfa raw ("LGAX") mentre la catena ha il
    # sito canonico ("20"). Normalizzo entrambi i lati per far matchare il join.
    if "MAG_SITO_COD" in sm.columns:
        _amap_sm = get_sito_alias_map(spark, f"{BRONZE_CATALOG}.{SCHEMA}")
        sm = sm.withColumn("MAG_SITO_COD", normalize_sito(F.col("MAG_SITO_COD"), _amap_sm))

    # ── Aggregato struttura_mag (replica subquery sm_aggr nelle viste AS-IS) ──
    # Filtro di business STRM_TIPO_STRUTTURA=0 (struttura standard) — ST13.
    sm_filtered = sm.filter(F.coalesce(F.col("STRM_TIPO_STRUTTURA").cast("int"), F.lit(0)) == 0)

    join_keys = [
        F.col("ca.CATE_CORSIA")   == F.col("sm.STRM_CORSIA"),
        F.col("ca.CATE_COLONNA")  == F.col("sm.STRM_COLONNA"),
        F.col("ca.CATE_PIANO")    == F.col("sm.STRM_PIANO"),
        F.coalesce(F.col("ca.CATE_LIVELLO").cast("int"), F.lit(0)) ==
            F.coalesce(F.col("sm.STRM_LIVELLO").cast("int"), F.lit(0)),
        F.col("ca.MAG_SITO_COD")  == F.col("sm.MAG_SITO_COD"),
    ]

    sm_aggr = (
        ca.alias("ca").join(sm_filtered.alias("sm"), join_keys, "inner")
        .groupBy(
            F.col("sm.MAG_SITO_COD").alias("MAG_SITO_COD"),
            F.col("sm.STRM_FLAG_SERVIZIO").alias("STRM_FLAG_SERVIZIO"),
            F.col("sm.STRM_CORSIA").alias("STRM_CORSIA"),
            F.col("sm.STRM_COLONNA").alias("STRM_COLONNA"),
            F.col("sm.STRM_PIANO").alias("STRM_PIANO"),
            F.col("sm.STRM_LIVELLO").alias("STRM_LIVELLO"),
        )
        .agg(
            F.min(
                F.col("sm.STRM_NRO_MAX_PAL_AL").cast("int") *
                F.col("sm.STRM_NRO_MAX_PAL_LA").cast("int") *
                F.col("sm.STRM_NRO_MAX_PAL_PR").cast("int")
            ).alias("HXLXP"),
            F.count(F.lit(1)).alias("NUMRECS"),
        )
    )

    # ── Join catena ↔ sm_aggr (chiavi: corsia, colonna, piano, livello, sito) ─
    join_aggr_keys = [
        F.col("ca.CATE_CORSIA")   == F.col("sma.STRM_CORSIA"),
        F.col("ca.CATE_COLONNA")  == F.col("sma.STRM_COLONNA"),
        F.col("ca.CATE_PIANO")    == F.col("sma.STRM_PIANO"),
        F.coalesce(F.col("ca.CATE_LIVELLO").cast("int"), F.lit(0)) ==
            F.coalesce(F.col("sma.STRM_LIVELLO").cast("int"), F.lit(0)),
        F.col("ca.MAG_SITO_COD")  == F.col("sma.MAG_SITO_COD"),
    ]

    base = ca.alias("ca").join(sm_aggr.alias("sma"), join_aggr_keys, "inner")

    # ── Selezione comune picking/scorte (parametrizzata sul flag) ─────────────
    def build_branch(df, serv_flag_literal):
        return df.select(
            F.col("ca.MAG_SITO_COD"),
            F.col("ca.CATE_COD_MSI").alias("ART_COD"),
            F.col("ca.ART_RADICE_COD"),
            F.col("ca.ART_VAR_LOGIS_COD"),
            # GIORNO_RILEV_STOCK = data della rilevazione = giorno dello snapshot catena.
            # FIX (dati reali): ETL_DATINS non esiste nella catena reale -> uso _bronze_load_date.
            clean_dat_d(F.col("ca._bronze_load_date")).alias("GIORNO_RILEV_STOCK_ID"),
            # FIX (sessione test): day-key coerente con le altre GIORNO_*_ID -> int YYYYMMDD
            # (era lasciato raw DateType, causava DELTA_FAILED_TO_MERGE_FIELDS vs schema legacy string).
            clean_dat_d(F.col("ca.CATE_DATA_CARICO")).alias("GIORNO_CAR_ID"),
            F.col("ca.CATE_DATA_SCADENZA").alias("DATA_SCAD_STOCK"),
            F.col("ca.CATE_NRO_ETICHETTA").alias("NUM_ETICH"),
            F.lit("").alias("FORN_COD"),  # arricchimento da STO_TES_CARICHI (Gold)
            F.col("ca.CATE_CORSIA").alias("MAPPA_CORSIA"),
            F.col("ca.CATE_COLONNA").alias("MAPPA_COL"),
            F.col("ca.CATE_PIANO").alias("MAPPA_PIANO"),
            F.coalesce(F.col("ca.CATE_LIVELLO").cast("int"), F.lit(0)).alias("MAPPA_LIV"),
            F.lit(serv_flag_literal).alias("MAPPA_SERV_FLAG"),
            F.lit(0).alias("MAPPA_TIPO_LOCZ_COD"),  # TODO fn_get_mappa_locz_tipo_cod(serv,hxlxp,numrecs)
            F.col("sma.HXLXP").alias("MAPPA_HXLXP"),
            F.col("sma.NUMRECS").alias("MAPPA_NUMRECS"),
            F.col("ca.CATE_NRO_PICKING").alias("NUM_PICK"),
            F.col("ca.CATE_QTA_PEZZI").alias("PZ_STOCK"),
            F.col("ca.CATE_QTA_UF_RIL").alias("QTA_UF_STOCK"),
            F.lit(0).alias("ART_MODL_PES_COD"),
            F.lit(0).alias("NUM_IMB_STOCK"),
            F.lit(0.0).alias("VAL_STOCK_NET_ACQ"),
            F.lit(0.0).alias("VAL_STOCK_ULT_ACQ"),
            F.lit(0.0).alias("VAL_STOCK_MED_POND"),
            F.col("ca.CATE_NRO_CARICO").alias("NUMERO_CAR"),
            F.col("ca.CATE_NRO_ORDINE").alias("NUMERO_ORDINE"),
        )

    # ── RAMO PICKING: STRM_FLAG_SERVIZIO upper = 'SI' (ST13) ──────────────────
    picking = build_branch(
        base.filter(F.upper(F.coalesce(F.col("sma.STRM_FLAG_SERVIZIO"), F.lit("N0"))) == F.lit("SI")),
        "SI",
    )

    # ── RAMO SCORTE: STRM_FLAG_SERVIZIO upper != 'SI' (ST13) ──────────────────
    scorte = build_branch(
        base.filter(F.upper(F.coalesce(F.col("sma.STRM_FLAG_SERVIZIO"), F.lit("NO"))) != F.lit("SI")),
        "NO",
    )

    # ── UNION ALL: predicati mutuamente esclusivi -> no doppio conteggio (ST14) ─
    stock_df = picking.unionByName(scorte)

    # ── Arricchimento valori stock da CNDSTOSTOCK (ST16, replica WL3) ─────────
    # Valori VAL_STOCK_* = prezzo/valore unitario * QTA_UF. Join su articolo+sito.
    # PUNTO APERTO: la chiave di matching catena<->cndstostock (articolo vs etichetta)
    # va confermata col functional expert. Per ora left join non distruttivo su ART_COD.
    if spark.catalog.tableExists(SOURCE_CNDSTOCK):
        cnd = (spark.table(SOURCE_CNDSTOCK)
               .filter(F.col("_bronze_load_date") == F.lit(run_date))
               .select(
                   F.col("STKCINT").alias("_cnd_art"),
                   F.col("STKULTPRZNET").cast("decimal(18,4)").alias("_stk_prz_net"),
                   F.col("STKULTSTOCK").cast("decimal(18,4)").alias("_stk_ult"),
                   F.col("STKPMP").cast("decimal(18,4)").alias("_stk_pmp"),
               ).dropDuplicates(["_cnd_art"]))
        stock_df = (
            stock_df.alias("s")
            .join(cnd.alias("c"), F.col("s.ART_COD") == F.col("c._cnd_art"), "left")
            .withColumn("VAL_STOCK_NET_ACQ",
                        F.coalesce(F.col("_stk_prz_net") * F.col("s.QTA_UF_STOCK").cast("decimal(18,4)"), F.lit(0.0)))
            .withColumn("VAL_STOCK_ULT_ACQ",
                        F.coalesce(F.col("_stk_ult") * F.col("s.QTA_UF_STOCK").cast("decimal(18,4)"), F.lit(0.0)))
            .withColumn("VAL_STOCK_MED_POND",
                        F.coalesce(F.col("_stk_pmp") * F.col("s.QTA_UF_STOCK").cast("decimal(18,4)"), F.lit(0.0)))
            .drop("_cnd_art", "_stk_prz_net", "_stk_ult", "_stk_pmp")
        )
        logger.info("Arricchimento valori stock da cndstostock_clean applicato.")
    else:
        logger.warning(f"{SOURCE_CNDSTOCK} non esiste: VAL_STOCK_* restano a 0 (ST16 non arricchito).")

    silver_df = (
        stock_df
        .withColumn("_silver_ts", F.current_timestamp())
        .withColumn("_silver_load_date", F.lit(run_date).cast("date"))
    )

    check_not_null(silver_df, ["MAG_SITO_COD", "ART_COD"], NOTEBOOK_NAME)
    rows_clean = silver_df.count()
    logger.info(f"Righe silver (picking+scorte): {rows_clean}")
    check_row_count(silver_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    # SNAPSHOT: append partizionato per data rilevazione (stato giornaliero)
    # Idempotente: dynamic partition overwrite (no raddoppio su re-run stesso giorno).
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (silver_df.write.format("delta").mode("overwrite")
     .partitionBy("_silver_load_date").option("mergeSchema", "true")
     .saveAsTable(TARGET_TABLE))
    logger.info(f"SNAPSHOT append {TARGET_TABLE} ({rows_clean} righe per run_date={run_date})")

    logger.info(f"END {NOTEBOOK_NAME} | righe_catena={rows_catena} | righe_silver={rows_clean}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
