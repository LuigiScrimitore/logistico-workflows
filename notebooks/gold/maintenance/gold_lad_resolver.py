# Databricks notebook source
# Area: Gold — Manutenzione / LAD Resolver
# Versione: 1.1.0  Data: 2026-07-05
#   v1.1: config F_PREP_SPED completata (ART_RADICE_COD), CORRIERE_COD tolto da F_CARICO
#         (non-LAD, sentinel by-design), calcolo n_resolved pulito, retention/quarantena
#         orphan residui (widget retention_days) — OP-32.
# Tabella: (tutte le fact con colonne _COD_NAT)
#
# Prerequisito L-01: ogni fact gold espone <dim>_COD_NAT = codice naturale PRIMA
# della risoluzione surrogate. Questo notebook ri-risolve le righe FK='-1' che al
# momento dell'ingestion non avevano ancora la dimensione disponibile (LAD pattern).
#
# Quando eseguire: dopo un refresh di una o più dimensioni (LU_*), prima del batch
# analitico successivo. Il job è idempotente: ri-eseguirlo non cambia il risultato.
#
# Logica per ogni FK configurata:
#   - seleziona righe: FK == '-1'  AND  FK_NAT IS NOT NULL
#   - LEFT JOIN su dim per FK_NAT == dim_pk
#   - se match trovato → aggiorna FK con il valore risolto
#   - conta pre/post per reportistica
#
# Al termine: full overwrite della fact con mergeSchema=true (partitioning preservato
# tramite DESCRIBE DETAIL).

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from dq_helper import check_orphan_rate
from utils import get_catalog

from pyspark.sql import functions as F, DataFrame
import json
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")
dbutils.widgets.text("fact_table", "F_CARICO",
                     "Nome fact in gold.logistica (es. F_CARICO, F_PREP_SPED, ...)")
dbutils.widgets.text("retail_master_schema", "bronze_dev.condiviso",
                     "Schema lookup condiviso (workaround CDT_DW; OP-02 -> gold_prod.condiviso)")
dbutils.widgets.dropdown("dry_run", "false", ["false", "true"],
                         "Dry-run: conta orfani risolvibili senza scrivere")
dbutils.widgets.text("retention_days", "30",
                     "Finestra retention orphan (gg) prima della quarantena DQ")

env        = dbutils.widgets.get("env")
run_date   = dbutils.widgets.get("run_date")
fact_name  = dbutils.widgets.get("fact_table").strip().upper()
retail_ms  = dbutils.widgets.get("retail_master_schema").strip()
dry_run    = dbutils.widgets.get("dry_run") == "true"
retention_days = int(dbutils.widgets.get("retention_days") or "30")

# COMMAND ----------

NOTEBOOK_NAME = "gold_lad_resolver"
GOLD_CATALOG  = get_catalog("gold", env)
TARGET_TABLE  = f"{GOLD_CATALOG}.logistica.{fact_name}"

logger = get_logger(NOTEBOOK_NAME)

# ---------------------------------------------------------------------------
# Config LAD per fact: {fk_col, nat_col, dim_fqn, dim_pk}
# Allineata al surrogate_key_fallback nei rispettivi gold_f_*.py (L-01).
# ---------------------------------------------------------------------------
def build_lad_config(fact: str, gold_cat: str, retail: str) -> list:
    G = gold_cat
    R = retail
    configs = {
        "F_CARICO": [
            {"fk_col": "ART_RADICE_COD", "nat_col": "ART_RADICE_COD_NAT",
             "dim_fqn": f"{R}.LU_ART_RADICE",           "dim_pk": "ART_RADICE_COD"},
            {"fk_col": "FORNITORE_COD",  "nat_col": "FORNITORE_COD_NAT",
             "dim_fqn": f"{R}.LU_FORNITORE",             "dim_pk": "FORN_COD"},
            {"fk_col": "SITO_COD",       "nat_col": "SITO_COD_NAT",
             "dim_fqn": f"{G}.logistica.LU_SITO",        "dim_pk": "SITO_COD"},
            {"fk_col": "OPERATORE_COD",  "nat_col": "OPERATORE_COD_NAT",
             "dim_fqn": f"{G}.logistica.LU_OPERATORE",   "dim_pk": "OPERATORE_COD"},
            {"fk_col": "RICEVITORE_COD", "nat_col": "RICEVITORE_COD_NAT",
             "dim_fqn": f"{G}.logistica.LU_OPERATORE",   "dim_pk": "OPERATORE_COD"},
            # NB: CORRIERE_COD escluso — su F_CARICO il vettore è assente by-design (decisione
            # rimozione join LU_CORRIERE dai carichi); resta sentinel -1 con NAT null → non è LAD.
        ],
        "F_PREP_SPED": [
            {"fk_col": "PDV_COD",                "nat_col": "PDV_COD_NAT",
             "dim_fqn": f"{R}.LU_PDV",                   "dim_pk": "PDV_COD"},
            {"fk_col": "MAG_SITO_COD",           "nat_col": "MAG_SITO_COD_NAT",
             "dim_fqn": f"{G}.logistica.LU_SITO",        "dim_pk": "SITO_COD"},
            {"fk_col": "ART_RADICE_COD",         "nat_col": "ART_RADICE_COD_NAT",
             "dim_fqn": f"{R}.LU_ART_RADICE",            "dim_pk": "ART_RADICE_COD"},
            {"fk_col": "OPER_PREP_COD",          "nat_col": "OPER_PREP_COD_NAT",
             "dim_fqn": f"{G}.logistica.LU_OPERATORE",   "dim_pk": "OPERATORE_COD"},
            {"fk_col": "VETTORE_PRESU_SPED_COD", "nat_col": "VETTORE_PRESU_SPED_COD_NAT",
             "dim_fqn": f"{G}.logistica.LU_CORRIERE",    "dim_pk": "CORRIERE_COD"},
        ],
        "F_TURNO_PREP_SITO": [
            {"fk_col": "PDV_COD",                "nat_col": "PDV_COD_NAT",
             "dim_fqn": f"{R}.LU_PDV",                   "dim_pk": "PDV_COD"},
            {"fk_col": "SITO_COD",               "nat_col": "SITO_COD_NAT",
             "dim_fqn": f"{G}.logistica.LU_SITO",        "dim_pk": "SITO_COD"},
            {"fk_col": "PREPARATORE_COD",        "nat_col": "PREPARATORE_COD_NAT",
             "dim_fqn": f"{G}.logistica.LU_OPERATORE",   "dim_pk": "OPERATORE_COD"},
            {"fk_col": "AREA_MERCEOLOGICA_COD",  "nat_col": "AREA_MERCEOLOGICA_COD_NAT",
             "dim_fqn": f"{G}.logistica.LU_AREA_MERCL_LOGIS", "dim_pk": "COD_AREA_MERC"},
        ],
        "F_TRASPORTO": [
            {"fk_col": "MAG_SITO_COD",     "nat_col": "MAG_SITO_COD_NAT",
             "dim_fqn": f"{G}.logistica.LU_SITO",     "dim_pk": "SITO_COD"},
            {"fk_col": "VETTORE_SPED_COD", "nat_col": "VETTORE_SPED_COD_NAT",
             "dim_fqn": f"{G}.logistica.LU_CORRIERE", "dim_pk": "CORRIERE_COD"},
        ],
        "F_ORDINI": [
            {"fk_col": "FORNITORE_COD", "nat_col": "FORNITORE_COD_NAT",
             "dim_fqn": f"{R}.LU_FORNITORE",           "dim_pk": "FORN_COD"},
            {"fk_col": "SITO_COD",      "nat_col": "SITO_COD_NAT",
             "dim_fqn": f"{G}.logistica.LU_SITO",      "dim_pk": "SITO_COD"},
        ],
    }
    return configs.get(fact, [])


def resolve_one_fk(fact_df: DataFrame, fk_col: str, nat_col: str,
                   dim_fqn: str, dim_pk: str, default_val: str = "-1") -> tuple:
    """
    Re-risolve le righe orfane (fk_col == default_val AND nat_col IS NOT NULL).

    Restituisce (DataFrame aggiornato, n_orphans_before, n_resolved).
    """
    if nat_col not in fact_df.columns:
        logger.warning(f"Colonna NAT '{nat_col}' assente in {fact_df}, skip.")
        return fact_df, 0, 0

    is_orphan = (F.col(fk_col).cast("string") == F.lit(str(default_val))) & \
                F.col(nat_col).isNotNull()

    n_before = fact_df.filter(is_orphan).count()
    if n_before == 0:
        logger.info(f"  {fk_col}: nessun orfano con NAT valorizzato, skip.")
        return fact_df, 0, 0

    try:
        dim_keys = (spark.read.table(dim_fqn)
                    .select(F.col(dim_pk).cast("string").alias("_lad_pk"))
                    .distinct())
    except Exception as e:
        logger.warning(f"  Dimensione {dim_fqn} non disponibile: {str(e)[:80]}")
        return fact_df, n_before, 0

    joined = fact_df.join(dim_keys,
                          fact_df[nat_col].cast("string") == F.col("_lad_pk"),
                          "left")

    resolved_df = joined.withColumn(
        fk_col,
        F.when(is_orphan & F.col("_lad_pk").isNotNull(), F.col("_lad_pk"))
         .otherwise(F.col(fk_col))
    ).drop("_lad_pk")

    # Risolti = orfani (con NAT) che dopo il join non sono più al sentinel.
    n_residual = resolved_df.filter(
        (F.col(fk_col).cast("string") == F.lit(str(default_val))) &
        F.col(nat_col).isNotNull()
    ).count()
    n_resolved = n_before - n_residual

    logger.info(f"  {fk_col}: {n_before} orfani → {n_resolved} risolti "
                f"({n_before - n_resolved} residui non trovati in {dim_fqn})")
    return resolved_df, n_before, n_resolved


# COMMAND ----------

try:
    logger.info(f"START {NOTEBOOK_NAME} | env={env} | fact={fact_name} | dry_run={dry_run}")

    if not spark.catalog.tableExists(TARGET_TABLE):
        logger.warning(f"Tabella {TARGET_TABLE} non esiste. Notebook terminato.")
        dbutils.notebook.exit("NO_SOURCE")

    lad_config = build_lad_config(fact_name, GOLD_CATALOG, retail_ms)
    if not lad_config:
        logger.warning(f"Nessuna config LAD per '{fact_name}'. "
                       f"Fact supportate: F_CARICO, F_PREP_SPED, F_TURNO_PREP_SITO, "
                       f"F_TRASPORTO, F_ORDINI.")
        dbutils.notebook.exit("NO_LAD_CONFIG")

    # Legge la fact completa (LAD resolver agisce su tutto lo storico, non solo run_date).
    fact_df = spark.read.table(TARGET_TABLE)
    rows_total = fact_df.count()
    logger.info(f"Righe totali in {TARGET_TABLE}: {rows_total}")

    # Legge il partitioning esistente da Delta metadata per preservarlo in scrittura.
    detail = spark.sql(f"DESCRIBE DETAIL {TARGET_TABLE}").collect()[0]
    partition_cols = detail["partitionColumns"]
    logger.info(f"Partitioning esistente: {partition_cols}")

    # ── Risoluzione FK ────────────────────────────────────────────────────────
    total_orphans  = 0
    total_resolved = 0
    residuals = []   # (fk_col, n_residual) — candidati quarantena (NAT valorizzato ma non in dim)
    for cfg in lad_config:
        fact_df, n_bef, n_res = resolve_one_fk(
            fact_df,
            fk_col=cfg["fk_col"],
            nat_col=cfg["nat_col"],
            dim_fqn=cfg["dim_fqn"],
            dim_pk=cfg["dim_pk"],
        )
        total_orphans  += n_bef
        total_resolved += n_res
        if n_bef - n_res > 0:
            residuals.append((cfg["fk_col"], n_bef - n_res))

    logger.info(f"TOTALE: {total_orphans} orfani analizzati, {total_resolved} risolti")

    # ── Retention / quarantena orphan (punto 4 OP-32) ──────────────────────────
    # I residui = orphan con codice naturale valorizzato ma NON presente nella dim:
    # non sono late-arriving risolvibili, ma entità genuinamente assenti dal master
    # (spesso ART/FORN → dipende da OP-02 Retail Master). Oltre la finestra di retention
    # sono candidati a quarantena DQ. Qui li segnaliamo (non distruttivo).
    if residuals:
        det = ", ".join(f"{fk}={n}" for fk, n in residuals)
        logger.warning(f"QUARANTENA (candidati): {sum(n for _, n in residuals)} orphan residui "
                       f"con NAT valorizzato non trovati in dim [{det}] — retention {retention_days}gg, "
                       f"poi quarantena DQ. Per ART/FORN dipende da OP-02 (Retail Master).")

    # ── DQ post-risoluzione ───────────────────────────────────────────────────
    for cfg in lad_config:
        check_orphan_rate(fact_df, cfg["fk_col"], NOTEBOOK_NAME)

    # ── Scrittura ─────────────────────────────────────────────────────────────
    if dry_run:
        logger.info("DRY-RUN attivo: nessuna scrittura eseguita.")
        dbutils.notebook.exit(f"DRY_RUN orphans={total_orphans} resolved={total_resolved}")

    if total_resolved == 0:
        logger.info("Nessuna risoluzione nuova: scrittura saltata (tabella invariata).")
        dbutils.notebook.exit("NO_CHANGES")

    writer = (fact_df.write
              .format("delta")
              .mode("overwrite").option("partitionOverwriteMode", "dynamic")
              .option("mergeSchema", "true"))
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(TARGET_TABLE)

    logger.info(f"END {NOTEBOOK_NAME} | righe_scritte={rows_total} | "
                f"orfani_risolti={total_resolved}/{total_orphans}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
