# Databricks notebook source
# Area: Preparazione Spedizioni — Fact "prelievo" (legacy F_PREP_SPED)
# Layer: Silver PREP (Fase 1 modellazione + Fase 2 calcolo)  →  silver.logistica_curated.prep_sped
# Versione: 4.0.0
# Data: 2026-07-02
# Descrizione: STRATO PREP (standard 2-notebook, Linee guida §1-bis). Sostituisce silver_t_prep_sped.
#              FASE 1 (modellazione): OUTER join liste_uniche↔bolle_uniche su 8 chiavi
#                                     + LEFT JOIN riepiloghi/corsie/artdgene/aree_merceologiche.
#              FASE 2 (calcolo):      SEC_PREP_PREL (diff timestamp), valorizzazioni VAL_PREP_*,
#                                     chiavi-giorno clean_dat_d (date→int YYYYMMDD).
#              Grana = prelievo articolo (mag_sito, pdv, art, num_ord, num_riep, num_gabbia,
#                      seq_prel). Allineata a CDT_DW.INS_NEW_PREP_SPED (f_prep_sped).
#              Le surrogate key dimensionali NON si fanno qui: vivono nel Gold (Fase 3).
#
#              SORGENTI (silver.clean):
#                - silver.logistica.storico_liste_uniche  (prefisso LSPRL_)
#                - silver.logistica.storico_bolle_uniche  (prefisso BOL_)
#                - bronze.logistica.storico_riepiloghi (RPLPR_), corsie, artdgene, aree_merceologiche
#
#              CORREZIONI ANOMALIE LEGACY (Linee guida §6):
#                - SEC_PREP_PREL = unix_timestamp(fine) - unix_timestamp(inizio)  (NO formula DDD, S8)
#                - DATA_RIEP_INIZ/FINE = da RPLPR (NON SYSDATE, S9)
#                - vettore/autista/automezzo: presunto (da bolle) + reale (placeholder, S10)
#              MODE: DELTA_MERGE su grain-prelievo (MAG_SITO_COD, GIORNO_ORD_ID, SOCIO_COD, NUM_RIEP,
#                    NUM_GABBIA, NUM_ORD, ART_RADICE_COD, ART_VAR_LOGIS_COD, SEQ_PREL_PREP).
#              Riferimento: V_PREP_SPED_OUTER (VISTE:2687); SP_INS_T_PREP_SPED (CDT_ESTR:37944).
#              ALLINEAMENTO 2026-07-02 (certifica): articolo=radice+var logistica (no ART_COD);
#                droppate ORA_* (regola triple tempo); droppati VETTORE/AUTISTA/AUTOM_SPED (costanti/dismessi);
#                grain include SEQ_PREL_PREP.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from dq_helper import check_not_null, check_row_count
from utils import get_catalog, clean_dat_d, julian_to_date, art_radice, art_variante

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from functools import reduce
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")
# PATTERN #2 (incrementale per chiavi-impattate): in incrementale processa solo le chiavi
# di prelievo toccate dal batch del giorno (da liste_uniche O bolle_uniche con
# _silver_load_date == run_date), ri-join solo quelle, MERGE upsert. full_refresh -> tutto.
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"], "Full refresh")

env          = dbutils.widgets.get("env")
run_date     = dbutils.widgets.get("run_date")
full_refresh = dbutils.widgets.get("full_refresh") == "true"

# COMMAND ----------

NOTEBOOK_NAME    = "silver_prep_prep_sped"
BRONZE_CATALOG   = get_catalog("bronze", env)
SILVER_CATALOG   = get_catalog("silver", env)
SCHEMA           = "logistica"

# SORGENTI silver.clean / bronze
SRC_LISTE_UNICHE = f"{SILVER_CATALOG}.{SCHEMA}.storico_liste_uniche"
SRC_BOLLE_UNICHE = f"{SILVER_CATALOG}.{SCHEMA}.storico_bolle_uniche"
SRC_RIEPILOGHI   = f"{BRONZE_CATALOG}.{SCHEMA}.storico_riepiloghi"        # STAT (RPLPR_)
SRC_CORSIE       = f"{BRONZE_CATALOG}.{SCHEMA}.corsie"
SRC_ARTDGENE     = f"{BRONZE_CATALOG}.{SCHEMA}.artdgene"
SRC_AREE         = f"{BRONZE_CATALOG}.{SCHEMA}.aree_merceologiche"

# TARGET strato PREP
TARGET_TABLE     = f"{SILVER_CATALOG}.logistica_curated.prep_sped"

# 8 chiavi di prelievo (join liste-bolle).
JOIN_PAIRS = [
    ("LSPRL_SITO",           "BOL_SITO"),
    ("LSPRL_NRO_GABBIA",     "BOL_NRO_GABBIA"),
    ("LSPRL_NRO_ORDINE_NEG", "BOL_NRO_ORDINE_NEG"),
    ("LSPRL_COD_NEGOZIO",    "BOL_COD_NEGOZIO"),
    ("LSPRL_COD_MSI",        "BOL_COD_MSI"),
    ("LSPRL_DATA_ORDIN_NEG", "BOL_DATA_ORDIN_NEG"),
    ("LSPRL_SEQUE_PRELIEVO", "BOL_SEQUE_PRELIEVO"),
    ("LSPRL_FLAG_SCARTATO",  "BOL_FLAG_SCARTATO"),
]

logger = get_logger(NOTEBOOK_NAME)

def safe_table(name):
    return spark.table(name) if spark.catalog.tableExists(name) else None

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    for t in [SRC_LISTE_UNICHE, SRC_BOLLE_UNICHE]:
        if not spark.catalog.tableExists(t):
            logger.warning(f"Sorgente richiesta {t} non esiste. Notebook terminato.")
            dbutils.notebook.exit("NO_SOURCE")

    sl = spark.table(SRC_LISTE_UNICHE).alias("sl")
    sb = spark.table(SRC_BOLLE_UNICHE).alias("sb")

    # ── PATTERN #2: restringe il driver (liste) alle sole chiavi-prelievo impattate ──
    # Impattate = chiavi toccate oggi in liste_uniche O in bolle_uniche (le bolle riportate
    # ai nomi-liste). Un prep row cambia se la sua liste o la bolla agganciata e' cambiata.
    # (Limite noto: variazioni solo-riepilogo non ridisegnano qui; raro, enrichment LEFT.)
    incremental = (not full_refresh) and spark.catalog.tableExists(TARGET_TABLE)
    if incremental:
        _lk = [l for (l, _b) in JOIN_PAIRS]
        _rd = F.lit(run_date).cast("date")
        imp_l = spark.table(SRC_LISTE_UNICHE).filter(F.col("_silver_load_date") == _rd).select(*_lk)
        imp_b = (spark.table(SRC_BOLLE_UNICHE).filter(F.col("_silver_load_date") == _rd)
                 .select(*[F.col(b).alias(l) for (l, b) in JOIN_PAIRS]))
        impacted = imp_l.unionByName(imp_b).distinct().alias("imp")
        semicond = reduce(lambda a, b: a & b,
                          [F.col(f"sl.{l}").eqNullSafe(F.col(f"imp.{l}")) for l in _lk])
        sl = sl.join(impacted, semicond, "left_semi").alias("sl")
        logger.info(f"INCREMENTALE pattern #2: chiavi-prelievo impattate = {sl.count()}")

    rie_t = safe_table(SRC_RIEPILOGHI)
    cor_t = safe_table(SRC_CORSIE)
    art_t = safe_table(SRC_ARTDGENE)
    aree_t= safe_table(SRC_AREE)

    logger.info(f"Righe sorgenti UNICHE: liste={sl.count()} bolle={sb.count()}")

    # ── FASE 1 — STEP 1: V_PREP_SPED_OUTER = LEFT join liste_uniche ↔ bolle_uniche ──
    join_cond = [F.col(f"sl.{l}") == F.col(f"sb.{b}") for (l, b) in JOIN_PAIRS]
    outer_df = sl.join(sb, join_cond, "left")

    # OP-PSP-2: LSPRL_DATA_INIZIO/FINE_PRELIEVO sono NULL nella sorgente Logistix.
    # Ricostruire il timestamp di INIZIO da LSPRL_DATA_PRELIEVO (Julian) + LSPRL_ORA_PRELIEVO (HHMM),
    # fedele alla formula CDT_ESTR_VISTE.sql: TO_DATE((yyyymmdd||LPAD(ora,4,'0')),'YYYYMMDDHH24MI').
    # FINE non disponibile dalle liste; futura integrazione con riepiloghi (RPLPR_*).
    _di_date = julian_to_date(F.col("sl.LSPRL_DATA_PRELIEVO").cast("long"))
    _di_ora  = F.lpad(F.coalesce(F.col("sl.LSPRL_ORA_PRELIEVO"), F.lit("0")), 4, "0")
    di  = F.to_timestamp(F.concat(F.date_format(_di_date, "yyyyMMdd"), _di_ora), "yyyyMMddHHmm")
    df_ = F.lit(None).cast("timestamp")   # LSPRL_DATA_FINE_PRELIEVO assente

    # ── FASE 2 — SEC_PREP_PREL (S8): NULL se timestamp fine mancante.
    sec_prep_prel = F.lit(None).cast("long")

    wl2 = outer_df.select(
        F.col("sl.LSPRL_SITO").alias("MAG_SITO_COD"),
        F.col("sl.LSPRL_COD_NEGOZIO").alias("SOCIO_COD"),
        # Dimensione articolo: radice + variante LOGISTICA da MSI (OP-12), NIENTE ART_COD.
        art_radice(F.col("sl.LSPRL_COD_MSI")).alias("ART_RADICE_COD"),
        art_variante(F.col("sl.LSPRL_COD_MSI")).alias("ART_VAR_LOGIS_COD"),
        F.col("sl.LSPRL_NRO_ORDINE_NEG").alias("NUM_ORD"),
        clean_dat_d(F.col("sl.LSPRL_DATA_ORDIN_NEG")).alias("GIORNO_ORD_ID"),
        F.col("sl.LSPRL_NRO_RIEPILOGO").alias("NUM_RIEP"),
        F.col("sl.LSPRL_NRO_GABBIA").alias("NUM_GABBIA"),
        F.col("sl.LSPRL_SEQUE_PRELIEVO").alias("SEQ_PREL_PREP"),
        F.col("sl.LSPRL_FLAG_SCARTATO").alias("TIPO_SCAR_PREP_COD"),
        F.coalesce(F.col("sl.NUM_RIGHE"), F.lit(0)).alias("NUM_RIGHE_PREP"),
        # ── quantita' / valori ──
        F.col("sl.LSPRL_QTA_DA_EVADERE").alias("QTA_DAPREP"),
        F.col("sl.LSPRL_QTA_EVASA").alias("QTA_PREP"),
        (F.coalesce(F.col("sb.BOL_PRZ_ACQ_NETTO"), F.lit(0)) * F.coalesce(F.col("sl.LSPRL_QTA_EVASA"), F.lit(0))).alias("VAL_PREP_CST"),
        (F.coalesce(F.col("sb.BOL_PRZ_CESSIONE"), F.lit(0)) * F.coalesce(F.col("sl.LSPRL_QTA_EVASA"), F.lit(0))).alias("VAL_PREP_CES"),
        (F.coalesce(F.col("sb.BOL_PRZ_VENDITA"),  F.lit(0)) * F.coalesce(F.col("sl.LSPRL_QTA_EVASA"), F.lit(0))).alias("VAL_PREP_VEN"),
        # ── date/ore prelievo ──
        # Triple tempo: teniamo GIORNO_*_ID + DATA_* (timestamp), droppiamo ORA_* (regola pruning).
        clean_dat_d(di.cast("date")).alias("GIORNO_PREL_INIZ_ID"),
        di.alias("DATA_PREL_INIZ"),
        clean_dat_d(df_.cast("date")).alias("GIORNO_PREL_FINE_ID"),
        df_.alias("DATA_PREL_FINE"),
        # ── S8: durata prelievo da differenza timestamp ──
        sec_prep_prel.alias("SEC_PREP_PREL"),
        # ── S10: PRESUNTO (da bolle_uniche). Il vettore/autista/automezzo REALE (SPED) è
        #    costante/null in CDT_DW sul 2026 (dismesso) -> escluso dal fatto (regola pruning).
        F.coalesce(F.col("sb.BOL_COD_VETTORE"),   F.lit(0)).alias("VETTORE_PRESU_SPED_COD"),
        F.coalesce(F.col("sb.BOL_COD_AUTISTA"),   F.lit(0)).alias("AUTISTA_PRESU_SPED_COD"),
        F.coalesce(F.col("sb.BOL_COD_AUTOMEZZO"), F.lit(0)).alias("AUTOM_PRESU_SPED_COD"),
        F.coalesce(F.col("sb.BOL_NRO_BOLLA"), F.lit(0)).alias("NUM_BOLLA_SPED"),
        F.coalesce(F.col("sb.BOL_DATA_BOLLA"), F.lit(None).cast("date")).alias("DATA_BOLLA_SPED"),
        F.coalesce(F.col("sb.BOL_SPEDIZIONIERE"), F.lit(0)).alias("OPER_SPED_COD"),
        # ── anagrafici/posizionali ──
        F.col("sl.LSPRL_COD_PREPARATOR").alias("OPER_PREP_COD"),
        F.col("sl.LSPRL_CORSIA").alias("MAPPA_CORSIA"),
        F.coalesce(F.col("sl.LSPRL_COD_AREA_MERCEOLOGICA"), F.lit("-")).alias("AREA_MERCL_LOGIS_COD"),
    )

    # ── FASE 1 — STEP 2: + LEFT JOIN storico_riepiloghi (S9: date da RPLPR, non SYSDATE) ──
    if rie_t is not None:
        rie = rie_t.alias("rie")
        wl3 = wl2.alias("wl2").join(
            rie,
            [
                F.col("wl2.MAG_SITO_COD") == F.col("rie.RPLPR_SITO"),
                F.col("wl2.NUM_RIEP")     == F.col("rie.RPLPR_NRO_RIEPILOGO"),
                F.col("wl2.SOCIO_COD")    == F.col("rie.RPLPR_COD_NEGOZIO"),
            ],
            "left",
        ).select(
            "wl2.*",
            # FIX (dati reali): RPLPR_DATA_* sono JULIAN (es. 2461195) -> julian_to_date PRIMA.
            # Usare clean_dat_d direttamente castava il numero julian a data estrema -> long
            # overflow in scrittura Parquet (rebase LEGACY).
            clean_dat_d(julian_to_date(F.col("rie.RPLPR_DATA_INIZ_PREP"))).alias("GIORNO_RIEP_INIZ_ID"),
            clean_dat_d(julian_to_date(F.col("rie.RPLPR_DATA_FINE_PREP"))).alias("GIORNO_RIEP_FINE_ID"),
            # S9: DATA_RIEP_INIZ/FINE da RPLPR (NON SYSDATE), julian -> date
            julian_to_date(F.col("rie.RPLPR_DATA_INIZ_PREP")).alias("DATA_RIEP_INIZ"),
            julian_to_date(F.col("rie.RPLPR_DATA_FINE_PREP")).alias("DATA_RIEP_FINE"),
        )
    else:
        wl3 = (wl2.withColumn("GIORNO_RIEP_INIZ_ID", F.lit(0))
                  .withColumn("GIORNO_RIEP_FINE_ID", F.lit(0))
                  .withColumn("DATA_RIEP_INIZ", F.lit(None).cast("date"))
                  .withColumn("DATA_RIEP_FINE", F.lit(None).cast("date")))

    # NB (2026-06-10): rimossi i LEFT JOIN corsie/artdgene/aree_merceologiche.
    # Erano dead-join (la select finale prende solo wl3.*, nessuna colonna arricchita
    # veniva propagata) e artdgene non e' piu' raggiungibile/ingerita. Se in futuro
    # servono attributi corsia/area, riaggiungere il join SELEZIONANDO le colonne.
    prep_df = (
        wl3
         .withColumn("_silver_ts", F.current_timestamp())
         .withColumn("_silver_load_date", F.lit(run_date).cast("date"))
    )

    # ── DQ S8: SEC_PREP_PREL >= 0 (durata non negativa) ──
    neg_sec = prep_df.filter(F.col("SEC_PREP_PREL") < 0).count()
    if neg_sec > 0:
        logger.warning(f"DQ S8 FAILED: SEC_PREP_PREL < 0 in {neg_sec} righe (fine < inizio)")
    else:
        logger.info("DQ S8 PASSED: SEC_PREP_PREL >= 0")

    check_not_null(prep_df,
        ["MAG_SITO_COD", "SOCIO_COD", "ART_RADICE_COD", "GIORNO_ORD_ID", "NUM_RIEP", "NUM_GABBIA", "NUM_ORD"],
        NOTEBOOK_NAME)
    rows_clean = prep_df.count()
    logger.info(f"Righe prep_sped: {rows_clean}")
    check_row_count(prep_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    # Grain-prelievo ODI completo: include SEQ_PREL_PREP + variante logistica (art come radice+var).
    # (Prima ometteva SEQ_PREL_PREP -> ~30k righe collassate; ora allineato al grain documentato.)
    MERGE_KEYS = ["MAG_SITO_COD", "GIORNO_ORD_ID", "SOCIO_COD", "NUM_RIEP", "NUM_GABBIA",
                  "NUM_ORD", "ART_RADICE_COD", "ART_VAR_LOGIS_COD", "SEQ_PREL_PREP"]

    # Dedup interno deterministico su merge keys (il left join puo' duplicare multi-match).
    w = Window.partitionBy(*MERGE_KEYS).orderBy(
        F.col("_silver_ts").desc(),
        F.col("TIPO_SCAR_PREP_COD").asc_nulls_last(),
        F.col("NUM_BOLLA_SPED").asc_nulls_last(),
    )
    prep_df = prep_df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    rows_dedup = prep_df.count()
    if rows_dedup != rows_clean:
        logger.info(f"Dedup merge_keys: {rows_clean} -> {rows_dedup}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_CATALOG}.logistica_curated")

    if not spark.catalog.tableExists(TARGET_TABLE):
        logger.info(f"Prima esecuzione — CTAS {TARGET_TABLE}")
        prep_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)
    else:
        cond = " AND ".join([f"tgt.{k} <=> src.{k}" for k in MERGE_KEYS])
        DeltaTable.forName(spark, TARGET_TABLE).alias("tgt").merge(
            prep_df.alias("src"), cond
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.info(f"MERGE INTO {TARGET_TABLE} completato")

    logger.info(f"END {NOTEBOOK_NAME} | righe_prep={rows_dedup}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
