# Databricks notebook source
# Area: CARICHI — Bronze, recupero storico una tantum (ACT_9038)
# Versione: 1.0.0  Data: 2026-10-30
# Descrizione: integra nel bronze (sto_tes_carichi, sto_righe_carico, pesate) le righe presenti in Oracle ma mai
#   arrivate in landing perché l'estrattore deduplicava per merge_keys (spec_legacy_f_carico, famiglie A/B).
#   Sorgente: tabelle di appoggio bronze_<env>.logistica._act9038_<tabella> (estrazione Oracle in sola lettura,
#   carichi 25/08–31/10/2026, nessuna dedup). Inserisce SOLO le righe la cui chiave non è già nel bronze
#   (le righe già viste in landing tengono i loro giorni di vista reali). Giorni di vista delle righe integrate:
#   _first_seen_date = min(giorno timbro testata, prima vista della testata), _last_seen_date = data del recupero
#   (esistono ancora in Oracle) → risultano presenti prima e dopo il giro legacy del loro carico.
#   Da lanciare DOPO il ricaricamento del bronze dalla landing e PRIMA del full refresh del silver.

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")

from logging_helper import get_logger
from utils import get_catalog, add_row_hash
from pyspark.sql import functions as F
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
env = dbutils.widgets.get("env")
NOTEBOOK_NAME = "backfill_act9038_integra_da_oracle"
B = f"{get_catalog('bronze', env)}.logistica"
logger = get_logger(NOTEBOOK_NAME)

# stesse chiavi dei notebook bronze (ACT_9038)
TABELLE = {
    "sto_tes_carichi":  ["MAG_SITO_COD", "STCAR_NRO_CARICO", "STCAR_DATA_CARICO", "STCAR_NRO_ORDINE", "STCAR_COD_MAGAZZINO"],
    "sto_righe_carico": ["MAG_SITO_COD", "SRCAR_NRO_CARICO", "SRCAR_DATA_CARICO", "SRCAR_NRO_ORDINE", "SRCAR_COD_MSI",
                         "SRCAR_NRO_BOLLA_FORN", "SRCAR_COD_MAGAZZINO"],
    "pesate":           ["MAG_SITO_COD", "PSP_NRCARLOG", "PSP_NUMETIC", "PSP_NUMBOL"],
}
CARICO_COL = {"sto_tes_carichi": "STCAR_NRO_CARICO", "sto_righe_carico": "SRCAR_NRO_CARICO", "pesate": "PSP_NRCARLOG"}

# COMMAND ----------

# giorno di consumo per carico: timbro della testata (Oracle) + prima vista della testata nel bronze (se c'è)
ot = spark.read.table(f"{B}._act9038_sto_tes_carichi")
stamp = F.regexp_replace(F.col("STCAR_DATA_ESTRAZIONE_DWH").cast("string"), r"\.0+$", "")
cons = (ot.withColumn("_D", F.when(F.length(stamp) >= 12, F.to_date(F.substring(stamp, 1, 8), "yyyyMMdd")))
          .filter(F.col("_D").isNotNull())
          .groupBy(F.col("MAG_SITO_COD").alias("_c_sito"), F.col("STCAR_NRO_CARICO").alias("_c_carico"))
          .agg(F.max("_D").alias("_D")))
bt_first = (spark.read.table(f"{B}.sto_tes_carichi")
            .groupBy(F.col("MAG_SITO_COD").alias("_c_sito"), F.col("STCAR_NRO_CARICO").alias("_c_carico"))
            .agg(F.min("_first_seen_date").alias("_tfirst")))
cons = cons.join(bt_first, ["_c_sito", "_c_carico"], "left")

for tab, keys in TABELLE.items():
    target = f"{B}.{tab}"
    tcols = spark.read.table(target).columns
    src = spark.read.table(f"{B}._act9038_{tab}")
    src = (src.join(cons, (F.col("MAG_SITO_COD") == F.col("_c_sito")) & (F.col(CARICO_COL[tab]) == F.col("_c_carico")), "inner")
              .join(spark.read.table(target).select(*keys), keys, "left_anti"))
    if tab == "pesate":
        # pesata rinumerata dopo il consumo: in Oracle c'è solo il numero nuovo, nel bronze già quello vecchio (stessa
        # pesata: sito, carico, MSI, colli, UF, bolla) → non va inserita, sarebbe contata due volte. Le seconde pesate
        # delle etichette multi-bolla hanno bolla diversa e restano.
        # La gemella conta solo se la sua etichetta NON esiste più in Oracle (= numero vecchio, rinumerato): due pallet
        # identici che esistono entrambi in Oracle sono pesate distinte e restano.
        TW = ["MAG_SITO_COD", "PSP_NRCARLOG", "PSP_ARTEAN13", "PSP_NRCOLLI", "PSP_QTA_UF_RIL", "PSP_NUMBOL"]
        in_oracle = spark.read.table(f"{B}._act9038_pesate").select("MAG_SITO_COD", "PSP_NRCARLOG", "PSP_NUMETIC").distinct()
        gemelle = (spark.read.table(target).select(*TW, "PSP_NUMETIC")
                   .join(in_oracle, ["MAG_SITO_COD", "PSP_NRCARLOG", "PSP_NUMETIC"], "left_anti")
                   .select(*TW).distinct())
        src = src.join(gemelle, TW, "left_anti")
    # presenti in Oracle oggi → viste in continuo dal giro di consumo a oggi (ultima vista = data del recupero)
    src = (src.withColumn("_first_seen_date", F.least(F.col("_D"), F.coalesce(F.col("_tfirst"), F.col("_D"))))
              .withColumn("_last_seen_date", F.current_date())
              .withColumn("_bronze_load_date", F.col("_D"))
              .withColumn("_bronze_insert_ts", F.current_timestamp())
              .withColumn("_source_file", F.lit("oracle_backfill_ACT_9038"))
              .withColumn("_sito_cod", F.lower(F.col("MAG_SITO_COD"))))
    src = add_row_hash(src.select(*[c for c in tcols if c in src.columns and c != "_row_hash"]))
    src = src.dropDuplicates(keys)
    n = src.count()
    src.select(*[c for c in tcols if c in src.columns]).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target)
    logger.info(f"{tab}: integrate {n} righe da Oracle (assenti dalla landing)")
    print(f"{tab}: integrate {n} righe")

# COMMAND ----------

# timbro di consumo: righe viste da noi solo prima del giro legacy (timbro vuoto) ma timbrate in Oracle → il legacy
# le ha lette. Si riporta il timbro Oracle (solo dove il bronze non ne ha uno valido). Le righe sparite da Oracle
# (cancellate o rinumerate) restano senza timbro.
STAMP = {"sto_tes_carichi": "STCAR_DATA_ESTRAZIONE_DWH", "pesate": "PSP_DATA_ESTRAZIONE_DWH"}
for tab, col in STAMP.items():
    keys = TABELLE[tab]
    on = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
    # timbro valido = YYYYMMDDHHMI (>= 1e11); confronto numerico, niente regex nella SQL
    valido = lambda a: f"try_cast({a}.{col} as double) >= 100000000000"
    spark.sql(f"""
        MERGE INTO {B}.{tab} t
        USING (SELECT {', '.join(keys)}, {col} FROM {B}._act9038_{tab}) s
        ON {on}
        WHEN MATCHED AND NOT coalesce({valido('t')}, false) AND {valido('s')} THEN UPDATE SET t.{col} = s.{col}
    """)
    logger.info(f"{tab}: timbro di consumo riportato da Oracle sulle righe senza timbro")
    print(f"{tab}: timbro riportato da Oracle")

# COMMAND ----------

# righe/pesate che esistono in Oracle ma che l'estrattore ha mostrato in ritardo (dedup per merge_keys: ogni giorno
# passava una sola delle righe con la stessa chiave ridotta): sono presenti dalla prima vista del loro carico.
# Per le pesate decide comunque il timbro; per le righe una riga in più nello stesso gruppo non aggiunge righe al fatto.
for tab in ("sto_righe_carico", "pesate"):
    keys = TABELLE[tab]
    s = (spark.read.table(f"{B}._act9038_{tab}").select(*keys)
         .join(cons, (F.col("MAG_SITO_COD") == F.col("_c_sito")) & (F.col(CARICO_COL[tab]) == F.col("_c_carico")), "inner")
         .withColumn("_prima", F.least(F.col("_D"), F.coalesce(F.col("_tfirst"), F.col("_D"))))
         .select(*keys, "_prima").dropDuplicates(keys))
    s.createOrReplaceTempView("_ora_prima")
    on = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
    # esistono in Oracle oggi → ultima vista = oggi. Prima vista anticipata solo per le righe di dettaglio: per le
    # pesate la prima vista serve a riconoscere le etichette rinumerate dopo il giro e non va toccata.
    nuova_prima = "least(t._first_seen_date, s._prima)" if tab == "sto_righe_carico" else "t._first_seen_date"
    spark.sql(f"""
        MERGE INTO {B}.{tab} t USING _ora_prima s ON {on}
        WHEN MATCHED AND ({nuova_prima} < t._first_seen_date OR t._last_seen_date < current_date())
        THEN UPDATE SET t._first_seen_date = {nuova_prima}, t._last_seen_date = current_date()
    """)
    logger.info(f"{tab}: prima vista anticipata per le righe presenti in Oracle mostrate in ritardo dall'estrattore")
    print(f"{tab}: prima vista anticipata")
