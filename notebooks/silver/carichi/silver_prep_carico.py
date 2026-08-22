# Databricks notebook source
# Area: Carichi — Fact carico (grain ETICHETTA)
# Layer: Silver PREP (Fase 1 modellazione + Fase 2 calcolo)  →  silver.logistica_curated.carico
# Versione: 2.0.0
# Data: 2026-07-02
# Descrizione: STRATO PREP — port fedele della logica ODI CDT_ESTR.V_CARICO_ORDINARIO
#              (fonte reale: DOCS/99. SCRIPT/CDT_ESTR_VISTE.sql + CDT_ESTR.sql).
#
#   GRAIN = 1 riga per ETICHETTA (NUM_ETICH), guidato dalle PESATE (non piu' testata⋈dettaglio).
#
#   MODELLAZIONE (= V_CARICO_ORDINARIO):
#     r = carico_dettaglio AGGREGATO per (SITO,CARICO,DATA,ORDINE,MSI,RADICE,VAR,BOLLA)
#         con MAX() su imballi/ora/date, MIN(RICEVITORE).
#     p = pesata (grain driver: NUM_ETICH = ETICHET_NRO).
#     t = carico_testata (attributi di testata).
#     JOIN p⋈r su (SITO, CARICO=CARICO_LOG_NRO, BOLLA=BOLLA_NRO, ORDINE=COMMESSA_NRO,
#                  MSI=ART_EAN13)  [PSP_ARTEAN13 e' un codice MSI, non un EAN — overlap 100%].
#     JOIN t⋈r su (SITO, CARICO, DATA, ORDINE).
#
#   CALCOLO (misure dalla pesata):
#     QTA_UF_CARICO      = pesata.QTA_UF_RILEVATA
#     QTA_CARICO         = pesata.NRO_COLLI * pesata.PZ_PER_CARTONE
#     NUM_IMB_CARICO     = pesata.NRO_COLLI
#     NUM_IMB_FORN_CARICO= QTA_CARICO / NULLIF(NUM_PZ_IMB_EFF_FORN,0)
#     NUM_PLT_CARICO     = 1
#     STESSO_IMB_CARICO_FLAG = (NUM_PZ_IMB_EFF_FORN == NUM_PZ_IMB_SITO)
#
#   Dimensione articolo (decisione 2026-07-02): esposti ART_RADICE_COD + ART_VARIANTE_LOGIS
#   (ART_VAR_LOGIS_COD). NIENTE ART_COD (obsoleto) ne' varianti storicizzate.
#
#   PES_CARICO / VOL_CARICO: NON qui — calcolati nel Gold da LU_ART_UNITA_LOGISTICA (formula ODI).
#   VAL_COSTO_CARICO: OPEN POINT OP-CAR-1 (sorgente cndstostock dismessa/2020) -> NULL.
#   QTA_ORD_FORN: OPEN POINT OP-CAR-3 (distribuzione WL4 CEIL/FLOOR degenere nel legacy) -> 0.
#
#   SORGENTI (silver.clean): carico_testata, carico_dettaglio, pesata.
#   MODE: FULL_OVERWRITE per ANNO_MESE (idempotenza mensile gestita a valle dal Gold).

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from dq_helper import check_row_count
from utils import get_catalog
from pyspark.sql import functions as F, Window
from pyspark.sql.types import IntegerType, DoubleType, StringType, DateType
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")

env      = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

NOTEBOOK_NAME  = "silver_prep_carico"
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SRC_TESTATA    = f"{SILVER_CATALOG}.{SCHEMA}.carico_testata"
SRC_DETTAGLIO  = f"{SILVER_CATALOG}.{SCHEMA}.carico_dettaglio"
SRC_PESATA     = f"{SILVER_CATALOG}.{SCHEMA}.pesata"
TARGET_TABLE   = f"{SILVER_CATALOG}.logistica_curated.carico"

ANNO_MESE = run_date[:4] + run_date[5:7]

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date} | ANNO_MESE={ANNO_MESE}")

    if (not spark.catalog.tableExists(SRC_TESTATA)
            or not spark.catalog.tableExists(SRC_DETTAGLIO)
            or not spark.catalog.tableExists(SRC_PESATA)):
        logger.warning("Sorgenti carico_testata/dettaglio/pesata assenti. Notebook terminato.")
        dbutils.notebook.exit("NO_SOURCE")

    df_t = (spark.read.table(SRC_TESTATA)
            .filter(F.date_format(F.col("DATA_CARICO"), "yyyyMM") == F.lit(ANNO_MESE)))
    df_d = spark.read.table(SRC_DETTAGLIO)
    df_p = spark.read.table(SRC_PESATA)

    # ── (r) dettaglio AGGREGATO a livello (sito,carico,data,ordine,msi,radice,var,bolla) ──
    #    Riproduce la subquery "r" di V_CARICO_ORDINARIO: MAX() su imballi/ora/date, MIN(ricevitore).
    r = (df_d.groupBy(
            "SITO_COD", "CARICO_NRO", "DATA_CARICO", "ORDINE_NRO",
            "MSI_COD", "ART_RADICE", "ART_VAR", "BOLLA_FORN_NRO",
         ).agg(
            F.max("ORA_CARICO").alias("ORA_CARICO"),
            F.min("RICEVITORE_COD").alias("RICEVITORE_COD"),
            # OP-CAR-3: quantita' ordinata fornitore (SRCAR_QTA_ORDINATA -> silver QTA_ORDINATA).
            # Portata qui per distribuirla a valle sulla prima etichetta del gruppo ordine-articolo.
            F.max("QTA_ORDINATA").alias("QTA_ORDINATA_SRC"),
            F.max("DATA_BOLLA_FORN").alias("DATA_BOLLA_FORN"),
            F.max("NUM_PZ_IMB_ORD_FORN").alias("NUM_PZ_IMB_ORD_FORN"),
            F.max("NUM_PZ_IMB_EFF_FORN").alias("NUM_PZ_IMB_EFF_FORN"),
            F.max("NUM_PZ_IMB_SITO").alias("NUM_PZ_IMB_SITO"),
            F.max("NUM_IMB_STRATO_PLT_SITO").alias("NUM_IMB_STRATO_PLT_SITO"),
            F.max("NUM_STRATO_PLT_SITO").alias("NUM_STRATO_PLT_SITO"),
            F.max("NUM_IMB_ULT_STRATO_SITO").alias("NUM_IMB_ULT_STRATO_SITO"),
            F.max("NUM_IMB_STRATO_PLT_FORN").alias("NUM_IMB_STRATO_PLT_FORN"),
            F.max("NUM_STRATO_PLT_FORN").alias("NUM_STRATO_PLT_FORN"),
            F.max("NUM_IMB_ULT_STRATO_FORN").alias("NUM_IMB_ULT_STRATO_FORN"),
         ))

    # ── (p ⋈ r) — la pesata guida il grain (1 riga per etichetta) ─────────────────
    #    PSP_ARTEAN13 (silver ART_EAN13) e' un MSI -> aggancio su MSI_COD del dettaglio.
    j = (df_p.alias("p")
         .join(r.alias("r"),
               (F.col("p.SITO_COD")        == F.col("r.SITO_COD")) &
               (F.col("p.CARICO_LOG_NRO")  == F.col("r.CARICO_NRO")) &
               (F.col("p.BOLLA_NRO")       == F.col("r.BOLLA_FORN_NRO")) &
               (F.col("p.COMMESSA_NRO")    == F.col("r.ORDINE_NRO")) &
               (F.col("p.ART_EAN13")       == F.col("r.MSI_COD")),
               "inner"))

    # ── (⋈ t) attributi di testata su (sito,carico,data,ordine) ───────────────────
    j = (j.join(df_t.alias("t"),
                (F.col("r.SITO_COD")   == F.col("t.SITO_COD")) &
                (F.col("r.CARICO_NRO") == F.col("t.CARICO_NRO")) &
                (F.col("r.DATA_CARICO")== F.col("t.DATA_CARICO")) &
                (F.col("r.ORDINE_NRO") == F.col("t.ORDINE_NRO")),
                "inner"))

    # ── FASE 2: calcolo misure (dalla pesata) + campi derivati ────────────────────
    ora_pad = F.lpad(F.coalesce(F.col("ORA_CARICO"), F.lit("0")), 4, "0")
    qta_carico = (F.col("p.NRO_COLLI").cast(DoubleType()) * F.col("p.PZ_PER_CARTONE").cast(DoubleType()))
    pz_eff_forn = F.col("r.NUM_PZ_IMB_EFF_FORN").cast(DoubleType())

    prep_df = (j
        .withColumn("ORA_CARICO", ora_pad)
        .withColumn("FASCIA_ORA_ID", F.substring(ora_pad, 1, 2))
        .withColumn("QTA_UF_CARICO", F.col("p.QTA_UF_RILEVATA").cast(DoubleType()))
        .withColumn("QTA_CARICO", qta_carico)
        .withColumn("NUM_IMB_CARICO", F.col("p.NRO_COLLI").cast(DoubleType()))
        .withColumn("NUM_IMB_FORN_CARICO",
                    qta_carico / F.when(pz_eff_forn == 0, F.lit(1.0)).otherwise(pz_eff_forn))
        .withColumn("NUM_PLT_CARICO", F.lit(1).cast(IntegerType()))
        .withColumn("STESSO_IMB_CARICO_FLAG",
                    F.when(pz_eff_forn == F.col("r.NUM_PZ_IMB_SITO").cast(DoubleType()),
                           F.lit(1)).otherwise(F.lit(0)).cast(IntegerType()))
        .withColumn("ANNO_MESE", F.date_format(F.col("r.DATA_CARICO"), "yyyyMM"))
        # ── OP-CAR-3 (opzione B): distribuzione QTA_ORD_FORN ─────────────────────────
        #    Legacy: SUM(QTA_ORD_FORN) per (sito,ordine,radice,var) = MAX_QTA_ORD (qta ordinata);
        #    lo split per-riga era rumore (formula WL4 degenere + residuo su prima riga).
        #    Equivalente pulito: la qta ordinata sta tutta sulla prima etichetta del gruppo, 0 sulle altre.
        .withColumn("_max_qta_ord",
                    F.max(F.col("r.QTA_ORDINATA_SRC")).over(
                        Window.partitionBy(F.col("r.SITO_COD"), F.col("r.ORDINE_NRO"),
                                           F.col("r.ART_RADICE"), F.col("r.ART_VAR"))))
        .withColumn("_rn_ord_grp",
                    F.row_number().over(
                        Window.partitionBy(F.col("r.SITO_COD"), F.col("r.ORDINE_NRO"),
                                           F.col("r.ART_RADICE"), F.col("r.ART_VAR"))
                        .orderBy(F.col("p.ETICHET_NRO").asc_nulls_last())))
        .select(
            F.col("r.SITO_COD").cast(StringType()).alias("SITO_COD"),
            F.col("r.DATA_CARICO").cast(DateType()).alias("DATA_CARICO"),
            F.col("p.ETICHET_NRO").cast(StringType()).alias("NUM_ETICH"),
            F.col("ORA_CARICO").cast(StringType()).alias("ORA_CARICO"),
            F.col("FASCIA_ORA_ID").cast(StringType()).alias("FASCIA_ORA_ID"),
            F.col("t.FORNITORE_COD").cast(StringType()).alias("FORNITORE_COD"),
            # Dimensione articolo: radice + variante LOGISTICA (no ART_COD, no storicizzate)
            F.col("r.ART_RADICE").cast(StringType()).alias("ART_RADICE_COD"),
            F.col("r.ART_VAR").cast(StringType()).alias("ART_VAR_LOGIS_COD"),
            F.col("t.OPERATORE_COD").cast(StringType()).alias("OPERATORE_COD"),      # oper. validante
            F.col("r.RICEVITORE_COD").cast(StringType()).alias("RICEVITORE_COD"),    # oper. ricevente
            F.col("t.CORRIERE_COD").cast(StringType()).alias("CORRIERE_COD"),        # vettore carico
            # Date (in CDT_DW sono FK GIORNO_*_ID)
            F.col("t.DATA_EMISSIONE_ORD").cast(DateType()).alias("DATA_EMISSIONE_ORD"),
            F.col("t.DATA_CONFERMA_ORD").cast(DateType()).alias("DATA_PREV_CONS_FORN"),
            F.col("r.DATA_BOLLA_FORN").cast(DateType()).alias("DATA_BOLLA_FORN"),
            F.col("p.DATA_SCADENZA").cast(DateType()).alias("DATA_SCAD_CARICO"),
            # Chiavi documento
            F.col("r.CARICO_NRO").cast(StringType()).alias("NUM_DOC_CARICO"),
            F.col("r.BOLLA_FORN_NRO").cast(StringType()).alias("NUM_BOLLA_FORN"),
            F.col("r.ORDINE_NRO").cast(StringType()).alias("NUM_ORD_FORN"),
            # OP-CAR-3 (opzione B): qta ordinata sulla prima etichetta del gruppo (sito,ordine,radice,var),
            # 0 sulle altre -> SUM per gruppo = MAX_QTA_ORD legacy (quadra vs CDT_DW).
            # ⚠️ UNITÀ = COLLI/imballi (NON pezzi). Per confronto con QTA_CARICO (pezzi) va
            #    convertita: pezzi_ordinati = QTA_ORD_FORN × NUM_PZ_IMB_ORD_FORN (vedi A_INBOUND).
            #    Nome ODI-standard mantenuto per fedeltà/quadratura CDT_DW (vedi OP naming fase 2).
            F.when(F.col("_rn_ord_grp") == 1, F.coalesce(F.col("_max_qta_ord"), F.lit(0.0)))
             .otherwise(F.lit(0.0)).cast(DoubleType()).alias("QTA_ORD_FORN"),
            # Struttura imballo/pallet (da dettaglio aggregato)
            F.col("r.NUM_PZ_IMB_ORD_FORN").cast(DoubleType()).alias("NUM_PZ_IMB_ORD_FORN"),
            F.col("r.NUM_PZ_IMB_EFF_FORN").cast(DoubleType()).alias("NUM_PZ_IMB_EFF_FORN"),
            F.col("r.NUM_PZ_IMB_SITO").cast(DoubleType()).alias("NUM_PZ_IMB_SITO"),
            F.col("STESSO_IMB_CARICO_FLAG"),
            F.col("r.NUM_IMB_STRATO_PLT_SITO").cast(DoubleType()).alias("NUM_IMB_STRATO_PLT_SITO"),
            F.col("r.NUM_STRATO_PLT_SITO").cast(DoubleType()).alias("NUM_STRATO_PLT_SITO"),
            F.col("r.NUM_IMB_ULT_STRATO_SITO").cast(DoubleType()).alias("NUM_IMB_ULT_STRATO_SITO"),
            F.col("r.NUM_IMB_STRATO_PLT_FORN").cast(DoubleType()).alias("NUM_IMB_STRATO_PLT_FORN"),
            F.col("r.NUM_STRATO_PLT_FORN").cast(DoubleType()).alias("NUM_STRATO_PLT_FORN"),
            F.col("r.NUM_IMB_ULT_STRATO_FORN").cast(DoubleType()).alias("NUM_IMB_ULT_STRATO_FORN"),
            # Misure carico (dalla pesata)
            # QTA_UF_CARICO = unità di fatturazione ricevute (psp_qta_uf_ril)
            F.col("QTA_UF_CARICO"),
            # ⚠️ QTA_CARICO UNITÀ = PEZZI (NRO_COLLI × PZ_PER_CARTONE). Ordinato omologo =
            #    QTA_ORD_FORN × NUM_PZ_IMB_ORD_FORN. Nome ODI-standard (fedeltà/quadratura CDT_DW).
            F.col("QTA_CARICO"),
            F.col("NUM_IMB_CARICO"),
            F.col("NUM_IMB_FORN_CARICO"),
            F.col("NUM_PLT_CARICO"),
            F.lit(None).cast(DoubleType()).alias("VAL_COSTO_CARICO"),  # OP-CAR-1: cndstostock dismesso
            F.col("ANNO_MESE").cast(StringType()).alias("ANNO_MESE"),
        )
        .withColumn("_silver_ts", F.current_timestamp())
        .withColumn("_silver_load_date", F.lit(run_date).cast("date"))
    )

    rows_out = prep_df.count()
    logger.info(f"Righe prep carico (grain etichetta) ANNO_MESE={ANNO_MESE}: {rows_out}")
    check_row_count(prep_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_CATALOG}.logistica_curated")
    target_exists = spark.catalog.tableExists(TARGET_TABLE)
    if not target_exists:
        # Prima creazione: overwriteSchema richiede static mode
        (prep_df.write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true").partitionBy("ANNO_MESE")
            .saveAsTable(TARGET_TABLE))
        logger.info(f"CTAS {TARGET_TABLE} ({rows_out} righe)")
    else:
        # Run successivi: dynamic partition overwrite (solo ANNO_MESE corrente)
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        (prep_df.write.format("delta").mode("overwrite")
            .partitionBy("ANNO_MESE")
            .saveAsTable(TARGET_TABLE))
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        logger.info(f"OVERWRITE (dyn) {TARGET_TABLE} ({rows_out} righe)")

    logger.info(f"END {NOTEBOOK_NAME} | righe={rows_out}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
