# Databricks notebook source
# Area: Data Quality — Gate di pipeline (task DQ standalone)
# Versione: 1.0.0  Data: 2026-08-04  (ACT_9010)
#
# PERCHE' ESISTE: `dq_monitor`/`acceptance` (KIT-03/04, ADR-0014) erano **librerie** senza
# entrypoint orchestrabile: la DQ girava dentro i singoli notebook, non come step del DAG.
# Questo notebook e' il task DQ *standalone* eseguito in coda ai workflow di dominio: applica
# i criteri dichiarativi di ACCEPTANCE_REGISTRY, persiste gli esiti in
# `config_<env>.etl.dq_results` (D1) e applica il **gate**.
#
# COSA FA
#   1. seleziona le pipeline da verificare (per nome o per wave)
#   2. per ognuna esegue run_smoke_test(): row_count, not_null, unique_keys (grana),
#      orphan-rate sul sentinel, misure non negative, anomalia volumi vs storico
#   3. persiste gli esiti + alert sui fallimenti (LogNotifier; webhook/email in cloud)
#   4. se `gate=true` e almeno un check BLOCKING e' fallito -> DQBlockingError => task FAILED
#      (il workflow si ferma e parte la notifica email del job)
#
# WIDGET
#   env         dev | prod
#   run_date    data logica del run (default: oggi)
#   pipelines   csv di chiavi del registry (es. "gold_f_carico,gold_f_prep_sped");
#               "*" = tutte le pipeline del registry
#   wave        filtro alternativo per wave (A, B, C, D, E, A-agg, ...); ignorato se
#               `pipelines` e' valorizzato con qualcosa di diverso da "*"
#   gate        "true" blocca il workflow sui BLOCKING; "false" solo osserva (shadow mode)
#
# NOTA: le pipeline non presenti nel registry vengono **segnalate e saltate** (non fanno
# fallire il task): il registry si popola incrementalmente.

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from datetime import date

from logging_helper import get_logger
from acceptance import ACCEPTANCE_REGISTRY, run_smoke_test
from dq_monitor import DQBlockingError

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")
dbutils.widgets.text("pipelines", "*", "Pipeline del registry (csv) oppure *")
dbutils.widgets.text("wave", "", "Filtro per wave (usato se pipelines=*)")
dbutils.widgets.dropdown("gate", "true", ["true", "false"], "Blocca su BLOCKING")

env       = dbutils.widgets.get("env")
run_date  = dbutils.widgets.get("run_date")
pip_arg   = (dbutils.widgets.get("pipelines") or "*").strip()
wave_arg  = (dbutils.widgets.get("wave") or "").strip()
gate_on   = dbutils.widgets.get("gate").lower() == "true"

NOTEBOOK_NAME = "dq_gate"
logger = get_logger(NOTEBOOK_NAME)

# COMMAND ----------

# ── Selezione delle pipeline da verificare ──────────────────────────────────
if pip_arg and pip_arg != "*":
    requested = [p.strip() for p in pip_arg.split(",") if p.strip()]
    missing   = [p for p in requested if p not in ACCEPTANCE_REGISTRY]
    selected  = [p for p in requested if p in ACCEPTANCE_REGISTRY]
else:
    missing  = []
    selected = sorted(
        k for k, c in ACCEPTANCE_REGISTRY.items()
        if not wave_arg or (c.wave or "") == wave_arg
    )

logger.info(f"START {NOTEBOOK_NAME} | env={env} | run_date={run_date} | gate={gate_on} "
            f"| wave='{wave_arg}' | pipeline selezionate={len(selected)}")
for p in missing:
    logger.warning(f"Pipeline '{p}' non presente in ACCEPTANCE_REGISTRY: saltata "
                   f"(criteri da definire — ACT_9010)")
if not selected:
    logger.warning("Nessuna pipeline da verificare: task terminato senza check.")
    dbutils.notebook.exit("NO_PIPELINES")

# COMMAND ----------

# ── Esecuzione dei criteri ──────────────────────────────────────────────────
# gate=False nel singolo smoke-test: si raccolgono TUTTI gli esiti e si decide alla fine,
# così un fallimento sulla prima pipeline non nasconde lo stato delle altre.
report, blocking_total, failed_total = [], 0, 0

for name in selected:
    criteria = ACCEPTANCE_REGISTRY[name]
    try:
        dq = run_smoke_test(spark, criteria, env=env, run_date=run_date,
                            gate=False, logger=logger)
        summary  = dq.summary()
        blocking = len(dq.blocking_failures())
        failed   = len(dq.failures())
        blocking_total += blocking
        failed_total   += failed
        report.append((name, criteria.table, "OK" if failed == 0 else "FAIL", failed, blocking))
        logger.info(f"DQ {name}: failed={failed} blocking={blocking} | {summary}")
    except Exception as e:  # noqa: BLE001 — un errore su una pipeline non ferma le altre
        blocking_total += 1
        report.append((name, criteria.table, "ERROR", None, None))
        logger.error(f"DQ {name}: errore durante i check: {e}", exc_info=True)

# COMMAND ----------

# ── Riepilogo + gate ────────────────────────────────────────────────────────
print("\n" + "=" * 78)
print(f"  DQ GATE — env={env} run_date={run_date} gate={'ON' if gate_on else 'OFF'}")
print("=" * 78)
print(f"{'PIPELINE':<38} {'TABELLA':<26} {'ESITO':<6} FAIL/BLOCK")
print("-" * 78)
for name, table, esito, failed, blocking in report:
    fb = "-" if failed is None else f"{failed}/{blocking}"
    print(f"{name:<38} {table:<26} {esito:<6} {fb}")
print("-" * 78)
print(f"Pipeline verificate: {len(report)} | check falliti: {failed_total} | BLOCKING: {blocking_total}")
if missing:
    print(f"Saltate (non in registry): {', '.join(missing)}")

logger.info(f"END {NOTEBOOK_NAME} | pipeline={len(report)} | failed={failed_total} "
            f"| blocking={blocking_total}")

if gate_on and blocking_total > 0:
    raise DQBlockingError(
        f"DQ gate: {blocking_total} check BLOCKING falliti su {len(report)} pipeline "
        f"(env={env}, run_date={run_date}). Dettaglio in config_{env}.etl.dq_results."
    )

dbutils.notebook.exit(f"OK failed={failed_total} blocking={blocking_total}")
