# Databricks notebook source
# Area: Preparazione Spedizioni
# Layer: Silver (elaborazione intermedia — replica WL "UNICHE")
# Versione: 1.0.0
# Data: 2026-06-09
# Descrizione: Replica WL1_STORICO_BOLLE_UNICHE = GROUP BY 8 chiavi di prelievo dalla
#              silver.logistica.storico_bolle_clean (gia' julian->date, sito normalizzato).
#
#              8 CHIAVI: BOL_SITO, BOL_NRO_GABBIA, BOL_NRO_ORDINE_NEG, BOL_COD_NEGOZIO,
#                        BOL_COD_MSI, BOL_DATA_ORDIN_NEG, BOL_SEQUE_PRELIEVO, BOL_FLAG_SCARTATO
#
#              AGGREGAZIONI (S7): num_righe=COUNT(*); quantita'=SUM; date/prezzi=MAX;
#                                 anagrafici/posizionali (presunto vettore/autista/automezzo)=MIN.
#                                 BOLLE legacy gia' usava MAX su date/prezzi (coerente con S7).
#              DQ (S7): COUNT(DISTINCT col)=1 sugli attributi costanti per chiave (WARNING).
#              MODE: FULL_OVERWRITE.
#              Riferimento: Revisione §6.2, §9-bis S7; Linee guida §4.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from dq_helper import check_row_count
from utils import get_catalog

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from functools import reduce
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")
# PATTERN #2 (incrementale per chiavi-impattate): vedi silver_storico_liste_uniche.
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"], "Full refresh")

env          = dbutils.widgets.get("env")
run_date     = dbutils.widgets.get("run_date")
full_refresh = dbutils.widgets.get("full_refresh") == "true"

# COMMAND ----------

NOTEBOOK_NAME  = "silver_storico_bolle_uniche"
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SOURCE_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.storico_bolle_clean"
TARGET_TABLE   = f"{SILVER_CATALOG}.{SCHEMA}.storico_bolle_uniche"

KEYS = [
    "BOL_SITO", "BOL_NRO_GABBIA", "BOL_NRO_ORDINE_NEG", "BOL_COD_NEGOZIO",
    "BOL_COD_MSI", "BOL_DATA_ORDIN_NEG", "BOL_SEQUE_PRELIEVO", "BOL_FLAG_SCARTATO",
]

# Override espliciti.
SUM_COLS = []                                   # nessuna quantita' tipica nelle bolle uniche
MAX_COLS = ["BOL_DATA_BOLLA", "BOL_DATA_PARTENZA",
            "BOL_PRZ_ACQ_NETTO", "BOL_PRZ_CESSIONE", "BOL_PRZ_VENDITA"]
MIN_COLS = []

# Anagrafici presunti (devono essere costanti per chiave -> MIN tecnico + DQ costanza).
CONST_HINT = ["BOL_COD_VETTORE", "BOL_COD_AUTISTA", "BOL_COD_AUTOMEZZO",
              "BOL_SPEDIZIONIERE", "BOL_NRO_BOLLA"]

logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date}")

    if not spark.catalog.tableExists(SOURCE_TABLE):
        logger.warning(f"Sorgente {SOURCE_TABLE} non esiste. Notebook terminato.")
        dbutils.notebook.exit("NO_SOURCE")

    src = spark.table(SOURCE_TABLE)
    rows_read = src.count()
    logger.info(f"Righe lette da {SOURCE_TABLE}: {rows_read}")
    if rows_read == 0:
        dbutils.notebook.exit("NO_DATA")

    all_cols = [c for c in src.columns if not c.startswith("_")]
    non_key  = [c for c in all_cols if c not in KEYS]

    def classify(col):
        if col in SUM_COLS:
            return "sum"
        if col in MAX_COLS:
            return "max"
        if col in MIN_COLS:
            return "min"
        u = col.upper()
        if "QTA" in u or "QUANT" in u:
            return "sum"
        if "DATA" in u or "PRZ" in u or "PREZZO" in u or "PESO" in u or "ORA" in u:
            return "max"
        return "min"

    constant_attrs = [c for c in non_key if classify(c) == "min"]

    agg_exprs = [F.count(F.lit(1)).alias("NUM_RIGHE")]
    for c in non_key:
        kind = classify(c)
        if kind == "sum":
            agg_exprs.append(F.sum(F.col(c).cast("decimal(18,4)")).alias(c))
        elif kind == "max":
            agg_exprs.append(F.max(F.col(c)).alias(c))
        else:
            agg_exprs.append(F.min(F.col(c)).alias(c))

    # ── Scope di ricalcolo: full oppure solo chiavi-impattate (PATTERN #2) ──────
    incremental = (not full_refresh) and spark.catalog.tableExists(TARGET_TABLE)
    if incremental:
        s = src.alias("s")
        # GUARD anti-degenerazione: le righe del batch del giorno vs il totale del clean.
        impacted_rows = src.filter(F.col("_silver_load_date") == F.lit(run_date).cast("date")).count()
        if impacted_rows == 0:
            logger.info(f"INCREMENTALE: 0 righe nel batch {run_date} -> nulla da ricalcolare")
            dbutils.notebook.exit("NO_DELTA")
        if impacted_rows > 0.5 * rows_read:
            # Tipico full-rebuild: quasi tutta la clean ha lo stesso _silver_load_date -> imp=tutte
            # le chiavi -> il join+cache materializzerebbe l'intero dataset (spill enorme). Full path.
            logger.info(f"INCREMENTALE degenere: {impacted_rows}/{rows_read} righe impattate (>50%) "
                        f"-> full path senza cache (evita spill)")
            incremental = False
            agg_src = src
        else:
            imp = (src.filter(F.col("_silver_load_date") == F.lit(run_date).cast("date"))
                   .select(*KEYS).distinct().alias("imp"))
            n_imp = imp.count()
            joincond = reduce(lambda a, b: a & b,
                              [F.col(f"s.{k}").eqNullSafe(F.col(f"imp.{k}")) for k in KEYS])
            # cache: agg_src e' riusato (DQ + groupBy + MERGE) -> evita di ricalcolare il join.
            agg_src = s.join(imp, joincond, "inner").select("s.*").cache()
            logger.info(f"INCREMENTALE pattern #2: {n_imp} chiavi impattate (batch {run_date})")
    else:
        agg_src = src

    # ── DQ S7: COUNT(DISTINCT)=1 sugli attributi costanti per chiave ──────────
    # Gira su agg_src (in incrementale = solo le chiavi impattate) -> rapido, non sul full.
    if constant_attrs:
        dq_n = agg_src.count()
        DQ_SAMPLE_THRESHOLD = 500_000
        if dq_n > DQ_SAMPLE_THRESHOLD:
            frac = DQ_SAMPLE_THRESHOLD / dq_n
            dq_src = agg_src.sample(False, frac, seed=42)
            logger.info(f"DQ S7 su campione ~{frac:.1%} ({dq_n} righe -> soglia {DQ_SAMPLE_THRESHOLD})")
        else:
            dq_src = agg_src
        dq_df = (dq_src.groupBy(*KEYS)
                 .agg(*[F.countDistinct(F.col(c)).alias(f"_ndist_{c}") for c in constant_attrs]))
        dq_tot = dq_df.agg(*[
            F.sum(F.when(F.col(f"_ndist_{c}") > 1, 1).otherwise(0)).alias(f"_viol_{c}")
            for c in constant_attrs
        ]).collect()[0].asDict()
        for c in constant_attrs:
            v = dq_tot.get(f"_viol_{c}", 0) or 0
            if v > 0:
                logger.warning(f"DQ S7 FAILED: '{c}' non costante in {v} gruppi-chiave (atteso COUNT(DISTINCT)=1)")
        logger.info(f"DQ S7 costanza: verificati {len(constant_attrs)} attributi")

    # DQ-03: flag _bolla_multipla = True dove BOL_NRO_BOLLA varia nella chiave di prelievo.
    # La chiave di 8 colonne è semanticamente corretta (replica WL1); BOL_NRO_BOLLA non è chiave
    # perché una gabbia/ordine può essere associata a più bolle. Il flag rende esplicito questo
    # caso per auditing downstream senza alterare il modello (DQ-01 analysis 2026-06-20).
    bolla_multipla_df = (
        agg_src.groupBy(*KEYS)
        .agg(F.countDistinct(F.col("BOL_NRO_BOLLA")).alias("_ndist_bolla"))
    )
    agg_src_flagged = agg_src.join(bolla_multipla_df, on=KEYS, how="left")

    uniche = (agg_src_flagged.groupBy(*KEYS)
              .agg(*agg_exprs,
                   F.max(F.col("_ndist_bolla")).alias("_ndist_bolla")))
    uniche = uniche.withColumn("_bolla_multipla", F.col("_ndist_bolla") > 1).drop("_ndist_bolla")

    silver_df = (
        uniche
        .withColumn("_silver_ts", F.current_timestamp())
        .withColumn("_silver_load_date", F.lit(run_date).cast("date"))
    )

    rows_out = silver_df.count()
    logger.info(f"Righe UNICHE: {rows_out} (da {rows_read} righe-articolo)")
    check_row_count(silver_df, min_rows=0, notebook_name=NOTEBOOK_NAME)

    if incremental:
        cond = " AND ".join(f"tgt.{k} <=> src.{k}" for k in KEYS)
        (DeltaTable.forName(spark, TARGET_TABLE).alias("tgt")
         .merge(silver_df.alias("src"), cond)
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        logger.info(f"MERGE upsert {TARGET_TABLE} ({rows_out} chiavi ricalcolate)")
    else:
        (silver_df.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(TARGET_TABLE))
        logger.info(f"FULL OVERWRITE {TARGET_TABLE} ({rows_out} righe)")

    logger.info(f"END {NOTEBOOK_NAME} | righe_in={rows_read} | righe_uniche={rows_out}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
