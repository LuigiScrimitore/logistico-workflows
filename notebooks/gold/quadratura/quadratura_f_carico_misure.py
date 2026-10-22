# Databricks notebook source
# Area: Quadratura — F_CARICO gold vs CDT_DW (ODI)
# Versione: 1.0.0  Data: 2026-10-21
# Scopo: quadratura MISURE tra gold.f_carico e la vecchia CDT_DW.F_CARICO (file giornalieri),
#        sulle ETICHETTE COMUNI, con grain configurabile (giorno×sito, drill radice+variante),
#        + copertura tracciata a parte (OP-QDR-1: il gold copre un sottoinsieme).
#
# INPUT:
#   - gold_<env>.logistica.F_CARICO (cloud)
#   - file ODI giornalieri (parquet) estratti da scripts/quadratura/extract_f_carico.py,
#     caricati su Volume: <odi_ref_base>/AAAA/MM/GG/f_carico.parquet
#     (colonne usate: MAG_SITO_COD, NUM_DOC_CARICO, NUM_ETICH, GIORNO_DT, le 6 misure,
#      ART_RADICE_COD, ART_VAR_LOGIS_COD)
# OUTPUT (append per giorno):
#   - gold_<env>.logistica_dm.quadratura_f_carico_misure     (grain × misura → odi, gold, delta, match)
#   - gold_<env>.logistica_dm.quadratura_f_carico_copertura  (giorno × sito → n_odi, n_gold, n_comuni, %)

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from utils import get_catalog
from pyspark.sql import functions as F
from datetime import date, datetime, timedelta

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("da", "", "Da (YYYY-MM-DD)")
dbutils.widgets.text("a", "", "A (YYYY-MM-DD)")
dbutils.widgets.dropdown("grain", "giorno_sito", ["giorno_sito", "articolo"], "Grain quadratura")
dbutils.widgets.text("odi_ref_base", "/Volumes/landing_dev/logistica/files/quadratura/f_carico/cdtdw",
                     "Base file ODI (Volume)")

env   = dbutils.widgets.get("env")
da    = dbutils.widgets.get("da")
a     = dbutils.widgets.get("a") or da
grain = dbutils.widgets.get("grain")
odi_base = dbutils.widgets.get("odi_ref_base").rstrip("/")

# COMMAND ----------

NOTEBOOK_NAME = "quadratura_f_carico_misure"
GOLD_CATALOG  = get_catalog("gold", env)
FACT          = f"{GOLD_CATALOG}.logistica.F_CARICO"
SCHEMA_OUT    = f"{GOLD_CATALOG}.logistica_dm"
TGT_MISURE    = f"{SCHEMA_OUT}.quadratura_f_carico_misure"
TGT_COPERT    = f"{SCHEMA_OUT}.quadratura_f_carico_copertura"

MEASURES = ["QTA_CARICO", "QTA_UF_CARICO", "QTA_ORD_FORN", "PES_CARICO", "VOL_CARICO", "VAL_COSTO_CARICO"]
# tolleranza MATCH: assoluta per i piccoli numeri + relativa per pesi/volumi (float su miliardi)
EPS_ABS = 0.5
EPS_REL = 1e-5

logger = get_logger(NOTEBOOK_NAME)

# dimensioni di grain
GRAIN_DIMS = ["DATA_CARICO", "SITO_COD"] + (["ART_RADICE_COD", "ART_VAR_LOGIS_COD"] if grain == "articolo" else [])

# COMMAND ----------

# normalizzazioni per allineare le chiavi ODI↔gold
def norm_sito(col):
    # MAG_SITO_COD ('0051A') / SITO_COD ('51') -> numerico 2 cifre '51'
    digits = F.regexp_replace(col.cast("string"), "[^0-9]", "")
    return F.when(digits == "", None).otherwise(F.lpad(digits.cast("int").cast("string"), 2, "0"))

def norm_num(col):
    # 12345 / 12345.0 / '12345.0' -> '12345'
    return F.when(col.isNull(), None).otherwise(col.cast("string").cast("double").cast("long").cast("string"))

# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | {da}..{a} | grain={grain}")
    if not da:
        dbutils.notebook.exit("NO_DATE")

    # ── giorni da elaborare + path ODI esistenti ───────────────────────────────
    d0 = datetime.strptime(da, "%Y-%m-%d").date()
    d1 = datetime.strptime(a, "%Y-%m-%d").date()
    paths, dd = [], d0
    while dd <= d1:
        p = f"{odi_base}/{dd.year:04d}/{dd.month:02d}/{dd.day:02d}/f_carico.parquet"
        try:
            dbutils.fs.ls(p); paths.append(p)
        except Exception:
            logger.warning(f"file ODI assente per {dd} ({p}) — giorno saltato")
        dd += timedelta(days=1)
    if not paths:
        dbutils.notebook.exit("NO_ODI_FILE")

    # ── ODI: normalizza chiavi + misure + radice/variante ──────────────────────
    odi = spark.read.parquet(*paths)
    odi = (odi
           .withColumn("DATA_CARICO", F.to_date("GIORNO_DT"))
           .withColumn("SITO_COD", norm_sito(F.col("MAG_SITO_COD")))
           .withColumn("NDOC", norm_num(F.col("NUM_DOC_CARICO")))
           .withColumn("NETI", norm_num(F.col("NUM_ETICH"))))
    for m in MEASURES:
        odi = odi.withColumn(m, F.coalesce(F.col(m).cast("double"), F.lit(0.0)))
    odi_keys = ["DATA_CARICO", "SITO_COD", "NDOC", "NETI", "ART_RADICE_COD", "ART_VAR_LOGIS_COD"] + MEASURES
    odi = odi.select(*odi_keys).dropDuplicates(["DATA_CARICO", "SITO_COD", "NDOC", "NETI"])

    # ── GOLD: stessa normalizzazione ───────────────────────────────────────────
    gold = (spark.read.table(FACT)
            .filter((F.col("DATA_CARICO") >= F.lit(da)) & (F.col("DATA_CARICO") <= F.lit(a)))
            .withColumn("SITO_COD_N", norm_sito(F.col("SITO_COD")))
            .withColumn("NDOC", norm_num(F.col("NUM_DOC_CARICO")))
            .withColumn("NETI", norm_num(F.col("NUM_ETICH"))))
    for m in MEASURES:
        gold = gold.withColumn(m, F.coalesce(F.col(m).cast("double"), F.lit(0.0)))
    gold = (gold.select(
                F.col("DATA_CARICO"), F.col("SITO_COD_N").alias("SITO_COD"), "NDOC", "NETI",
                F.col("ART_RADICE_COD"), F.col("ART_VAR_LOGIS_COD"), *MEASURES)
            .dropDuplicates(["DATA_CARICO", "SITO_COD", "NDOC", "NETI"]))

    KEY = ["DATA_CARICO", "SITO_COD", "NDOC", "NETI"]

    # ── COPERTURA (giorno×sito): quante etichette ODI, gold, comuni ────────────
    cov = (odi.select(*KEY).withColumn("_odi", F.lit(1))
           .join(gold.select(*KEY).withColumn("_gold", F.lit(1)), KEY, "full")
           .groupBy("DATA_CARICO", "SITO_COD")
           .agg(F.sum(F.when(F.col("_odi").isNotNull(), 1).otherwise(0)).alias("N_ODI"),
                F.sum(F.when(F.col("_gold").isNotNull(), 1).otherwise(0)).alias("N_GOLD"),
                F.sum(F.when(F.col("_odi").isNotNull() & F.col("_gold").isNotNull(), 1).otherwise(0)).alias("N_COMUNI")))
    cov = (cov.withColumn("COPERTURA_PCT",
                          F.round(F.when(F.col("N_ODI") > 0, F.col("N_COMUNI") / F.col("N_ODI") * 100).otherwise(None), 1))
              .withColumn("_quadratura_ts", F.current_timestamp()))

    # ── MISURE sulle etichette COMUNI, aggregate al grain scelto ───────────────
    common = (odi.alias("o").join(gold.alias("g"), KEY, "inner")
              .select(*[F.col("o." + d) if d in ("ART_RADICE_COD", "ART_VAR_LOGIS_COD") else F.col(d) for d in GRAIN_DIMS],
                      *[F.col("o." + m).alias(m + "_odi") for m in MEASURES],
                      *[F.col("g." + m).alias(m + "_gold") for m in MEASURES]))
    agg = common.groupBy(*GRAIN_DIMS).agg(
        F.count(F.lit(1)).alias("N_ETICHETTE"),
        *[F.sum(m + "_odi").alias(m + "_odi") for m in MEASURES],
        *[F.sum(m + "_gold").alias(m + "_gold") for m in MEASURES])

    # long format: una riga per (grain × misura)
    stacked = None
    for m in MEASURES:
        part = (agg.select(*GRAIN_DIMS, "N_ETICHETTE",
                           F.lit(m).alias("MISURA"),
                           F.col(m + "_odi").alias("VAL_ODI"),
                           F.col(m + "_gold").alias("VAL_GOLD"))
                .withColumn("DELTA", F.col("VAL_ODI") - F.col("VAL_GOLD"))
                .withColumn("MATCH",
                            (F.abs(F.col("DELTA")) <= F.lit(EPS_ABS)) |
                            (F.abs(F.col("DELTA")) <= F.lit(EPS_REL) *
                             F.greatest(F.abs(F.col("VAL_ODI")), F.abs(F.col("VAL_GOLD"))))))
        stacked = part if stacked is None else stacked.unionByName(part)
    stacked = stacked.withColumn("GRAIN", F.lit(grain)).withColumn("_quadratura_ts", F.current_timestamp())

    # ── scrittura (overwrite dinamico per giorno) ──────────────────────────────
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_OUT}")
    (stacked.write.format("delta").mode("overwrite")
        .option("partitionOverwriteMode", "dynamic").option("mergeSchema", "true")
        .partitionBy("DATA_CARICO").saveAsTable(TGT_MISURE))
    (cov.write.format("delta").mode("overwrite")
        .option("partitionOverwriteMode", "dynamic").option("mergeSchema", "true")
        .partitionBy("DATA_CARICO").saveAsTable(TGT_COPERT))

    nrow = stacked.count()
    logger.info(f"END {NOTEBOOK_NAME} | righe misure={nrow} | {TGT_MISURE} / {TGT_COPERT}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
