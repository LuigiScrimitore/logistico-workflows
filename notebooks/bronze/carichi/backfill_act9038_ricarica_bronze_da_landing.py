# Databricks notebook source
# Area: CARICHI — Bronze, ricaricamento storico una tantum (ACT_9038)
# Versione: 1.0.0  Data: 2026-10-30
# Descrizione: ricostruisce sto_tes_carichi / sto_righe_carico / pesate del bronze con la nuova chiave (identità
#   completa della riga) e i giorni di vista, leggendo in un colpo tutti i file di landing dell'intervallo.
#   Per ogni riga: _first_seen_date = primo giorno di file in cui compare, _last_seen_date = ultimo, valori
#   dall'ultimo file. Equivale a rigiocare i giorni uno per uno coi notebook bronze (che usano least/greatest).
#   Scrittura: overwrite della tabella (nuova versione Delta → RESTORE alla versione precedente per tornare indietro).
#   Poi: backfill_act9038_integra_da_oracle (righe mai arrivate in landing), silver full refresh, prep, gold.

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from utils import get_catalog, add_row_hash
from pyspark.sql import functions as F, Window

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("landing_base_path", "abfss://logisticolanding@stdevdataplatformweudata.dfs.core.windows.net", "Landing base path")
dbutils.widgets.text("anno_mese_glob", "2026/10", "Mesi di landing (glob, es. 2026/10 o 2026/{09,10})")
dbutils.widgets.text("tabelle", "sto_tes_carichi,sto_righe_carico,pesate", "Tabelle")
env      = dbutils.widgets.get("env")
base     = dbutils.widgets.get("landing_base_path").rstrip("/")
mesi     = dbutils.widgets.get("anno_mese_glob").strip()
tabelle  = [t.strip() for t in dbutils.widgets.get("tabelle").split(",") if t.strip()]
B = f"{get_catalog('bronze', env)}.logistica"
logger = get_logger("backfill_act9038_ricarica_bronze_da_landing")

# stesse chiavi dei notebook bronze (ACT_9038)
KEYS = {
    "sto_tes_carichi":  ["MAG_SITO_COD", "STCAR_NRO_CARICO", "STCAR_DATA_CARICO", "STCAR_NRO_ORDINE", "STCAR_COD_MAGAZZINO"],
    "sto_righe_carico": ["MAG_SITO_COD", "SRCAR_NRO_CARICO", "SRCAR_DATA_CARICO", "SRCAR_NRO_ORDINE", "SRCAR_COD_MSI",
                         "SRCAR_NRO_BOLLA_FORN", "SRCAR_COD_MAGAZZINO"],
    "pesate":           ["MAG_SITO_COD", "PSP_NRCARLOG", "PSP_NUMETIC", "PSP_NUMBOL"],
}

# COMMAND ----------

for tab in tabelle:
    target = f"{B}.{tab}"
    keys = KEYS[tab]
    # colonne business del bronze attuale (stesso SOURCE_COLS del notebook bronze)
    biz = [c for c in spark.read.table(target).columns if not c.startswith("_")]
    raw = (spark.read.option("header", "true").option("inferSchema", "false").option("sep", ";").option("encoding", "UTF-8")
           .csv(f"{base}/logistix-landing/*/{tab}/{mesi}/*/*.csv")
           .withColumn("_source_file", F.col("_metadata.file_path")))
    raw = (raw.withColumn("_giorno", F.to_date(F.regexp_extract("_source_file", rf"/{tab}/(\d{{4}}/\d{{2}}/\d{{2}})/", 1), "yyyy/MM/dd"))
              .withColumn("_sito_cod", F.regexp_extract("_source_file", r"/logistix-landing/([^/]+)/", 1)))
    raw = raw.select(*[c for c in biz if c in raw.columns], "_source_file", "_giorno", "_sito_cod")
    wk = Window.partitionBy(*keys)
    last = (raw.withColumn("_first_seen_date", F.min("_giorno").over(wk))
               .withColumn("_last_seen_date", F.max("_giorno").over(wk))
               .withColumn("_rn", F.row_number().over(wk.orderBy(F.col("_giorno").desc(), F.col("_source_file").desc())))
               .filter("_rn = 1").drop("_rn"))
    out = (last.withColumn("_bronze_load_date", F.col("_last_seen_date"))
               .withColumn("_bronze_insert_ts", F.current_timestamp())
               .drop("_giorno"))
    out = add_row_hash(out)
    n = out.count()
    (out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target))
    logger.info(f"{tab}: bronze ricaricato da landing {mesi} — {n} righe")
    print(f"{tab}: {n} righe")
