# Databricks notebook source
# Area: Condiviso / Retail Master (anagrafiche da cdt_dw push)
# Layer: Gold (lookup condivise)
# Versione: 1.1.0
# Data: 2026-07-02
# Descrizione: Pubblica le anagrafiche master del DWH legacy CDT_DW (L_*) come
#              lookup LU_* nel nostro schema isolato bronze_<env>.condiviso.
#              Decisione D2 (2026-07-02): isolamento totale dal DWH aziendale.
#              In futuro (OP-02), quando le anagrafiche saranno su Gold, si aggangerà
#              direttamente: basterà ripuntare retail_master_schema.
#
#              Sorgente: cdtdw-landing/<L_*> (push read-only da sorgente cdt_dw)
#              Target:   bronze_<env>.condiviso.LU_*  (FULL_OVERWRITE, stato corrente)
#              Mapping L_* -> LU_* allineato a Excel "Tabelle Target CDT_DW" / OP-02.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")
dbutils.widgets.text("landing_base_path", "abfss://logistica@<storage>.dfs.core.windows.net", "Landing base path")
dbutils.widgets.text("file_format", "auto", "csv | parquet | auto")
dbutils.widgets.text("target_schema", "bronze_dev.condiviso", "Schema target LU condivise (D2: bronze_<env>.condiviso)")

env               = dbutils.widgets.get("env")
run_date          = dbutils.widgets.get("run_date")
landing_base_path = dbutils.widgets.get("landing_base_path").rstrip("/")
file_format       = dbutils.widgets.get("file_format").strip().lower()
target_schema     = dbutils.widgets.get("target_schema").strip()

# COMMAND ----------

NOTEBOOK_NAME = "gold_lu_from_cdtdw"
logger = get_logger(NOTEBOOK_NAME)
year, month, day = run_date.split("-")

# Mapping tabella landing (legacy) -> nome LU target
LOOKUP_MAP = {
    "L_ART_RADICE":           "LU_ART_RADICE",
    "L_FORN":                 "LU_FORNITORE",
    "L_PDV":                  "LU_PDV",
    "L_GIORNO":               "LU_GIORNO",
    "L_MESE":                 "LU_MESE",
    # Unità logistica articolo (peso lordo + dimensioni pezzo + variante logistica):
    # base del calcolo ODI di PES_CARICO/VOL_CARICO in gold_f_carico (FASE 3b).
    "LU_ART_UNITA_LOGISTICA": "LU_ART_UNITA_LOGISTICA",
}

# Tabelle a estrazione INCREMENTALE (delta via AUDIT_ID nell'extractor): invece del
# FULL_OVERWRITE si fa MERGE (upsert) sulla chiave, cosi' un delta non azzera la baseline.
# La baseline (prima esecuzione) crea la tabella via CTAS; i delta successivi fanno upsert.
MERGE_KEYS = {
    "LU_ART_UNITA_LOGISTICA": ["ART_RADICE_COD", "ART_VARIANTE_LOGIS_COD"],
}

# COMMAND ----------

def landing_path(tab):
    return f"{landing_base_path}/cdtdw-landing/{tab}/{year}/{month}/{day}/"

def detect_format(path):
    if file_format != "auto":
        return file_format
    try:
        for f in dbutils.fs.ls(path):
            if f.name.endswith(".parquet"):
                return "parquet"
            if f.name.endswith(".csv"):
                return "csv"
    except Exception:
        pass
    return "csv"

def read_one(path):
    fmt = detect_format(path)
    if fmt == "parquet":
        return spark.read.format("parquet").load(path)
    # multiLine + quote/escape: alcune anagrafiche (es. L_FORN) hanno newline
    # embedded nei campi note/indirizzo -> senza multiLine il conteggio righe sballa.
    return (spark.read.option("header", "true").option("inferSchema", "false")
            .option("sep", ";").option("encoding", "UTF-8")
            .option("multiLine", "true").option("quote", '"').option("escape", '"')
            .csv(f"{path}*.csv"))

# COMMAND ----------

logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date} | target={target_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")

processed = []
for legacy_tab, lu_name in LOOKUP_MAP.items():
    path = landing_path(legacy_tab)
    try:
        raw = read_one(path)
    except AnalysisException:
        logger.warning(f"{legacy_tab}: file landing assente ({path}) — skip")
        continue

    df = (raw
          .withColumn("_lu_load_date", F.lit(run_date).cast("date"))
          .withColumn("_lu_insert_ts", F.current_timestamp())
          .withColumn("_lu_source", F.lit(f"CDT_DW.{legacy_tab} (workaround OP-02)")))

    n = df.count()
    target = f"{target_schema}.{lu_name}"
    keys = MERGE_KEYS.get(lu_name)
    if keys and spark.catalog.tableExists(target):
        # Delta incrementale: upsert sulla chiave (preserva la baseline).
        cond = " AND ".join(f"tgt.{k} <=> src.{k}" for k in keys)
        (DeltaTable.forName(spark, target).alias("tgt")
            .merge(df.alias("src"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
        logger.info(f"  {legacy_tab} -> {target} : {n} righe delta (MERGE su {keys})")
    else:
        # Full/baseline: FULL_OVERWRITE (o CTAS iniziale per le incrementali).
        (df.write.format("delta").mode("overwrite")
           .option("overwriteSchema", "true").saveAsTable(target))
        mode_lbl = "CTAS baseline" if keys else "FULL_OVERWRITE"
        logger.info(f"  {legacy_tab} -> {target} : {n} righe ({mode_lbl})")
    processed.append((lu_name, n))

logger.info(f"END {NOTEBOOK_NAME} | LU pubblicate: {len(processed)} | "
            + ", ".join(f"{k}={v}" for k, v in processed))
