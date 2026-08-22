# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ### Gold Fact Table Template
# MAGIC
# MAGIC **Scopo:** Costruzione tabella dei fatti Gold da Silver + dimensioni.
# MAGIC
# MAGIC **Pattern:**
# MAGIC 1. Leggi Silver fact source
# MAGIC 2. JOIN dimensioni con surrogate_key_fallback (Late-Arriving Dimensions)
# MAGIC 3. Calcola misure / aggregazioni
# MAGIC 4. DeltaHelper.replace_where su partizione data
# MAGIC
# MAGIC **Parametri widget:**
# MAGIC - `silver_catalog` — Catalogo Silver (es. "silver_dev")
# MAGIC - `silver_schema` — Schema Silver (es. "logistica")
# MAGIC - `silver_fact_table` — Tabella Silver dei fatti (es. "carichi_testate")
# MAGIC - `dim_catalog` — Catalogo dimensioni condivise (es. "silver_dev")
# MAGIC - `dim_schema` — Schema dimensioni (es. "condiviso")
# MAGIC - `target_catalog` — Catalogo Gold (es. "gold_prod")
# MAGIC - `target_schema` — Schema Gold (es. "logistica")
# MAGIC - `target_table` — Tabella Gold target (es. "fact_carichi")
# MAGIC - `partition_col` — Colonna partizione date (es. "DATA_CARICO")
# MAGIC - `run_date` — Data di run / partizione da ricalcolare (YYYY-MM-DD)

# COMMAND ----------

import time
import sys

sys.path.insert(0, "/Workspace/Repos/logistico/lib")

from pyspark.sql import functions as F

from logistica_utils import (
    Logger,
    DeltaHelper,
    DQHelper,
    get_run_date,
    surrogate_key_fallback,
    cast_decimal,
)

# COMMAND ----------
# MAGIC %md #### 1. Parametri widget

# COMMAND ----------

dbutils.widgets.text("silver_catalog", "silver_dev", "Catalogo Silver sorgente")
dbutils.widgets.text("silver_schema", "logistica", "Schema Silver")
dbutils.widgets.text("silver_fact_table", "carichi_testate", "Tabella Silver fatti")
dbutils.widgets.text("dim_catalog", "silver_dev", "Catalogo dimensioni")
dbutils.widgets.text("dim_schema", "condiviso", "Schema dimensioni")
dbutils.widgets.text("target_catalog", "gold_prod", "Catalogo Gold target")
dbutils.widgets.text("target_schema", "logistica", "Schema Gold target")
dbutils.widgets.text("target_table", "fact_carichi", "Tabella Gold target")
dbutils.widgets.text("partition_col", "DATA_CARICO", "Colonna partizione date")
dbutils.widgets.text("run_date", "", "Data partizione da ricalcolare (YYYY-MM-DD)")

SILVER_CATALOG = dbutils.widgets.get("silver_catalog")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")
SILVER_FACT_TABLE = dbutils.widgets.get("silver_fact_table")
DIM_CATALOG = dbutils.widgets.get("dim_catalog")
DIM_SCHEMA = dbutils.widgets.get("dim_schema")
TARGET_CATALOG = dbutils.widgets.get("target_catalog")
TARGET_SCHEMA = dbutils.widgets.get("target_schema")
TARGET_TABLE = dbutils.widgets.get("target_table")
PARTITION_COL = dbutils.widgets.get("partition_col")
RUN_DATE = dbutils.widgets.get("run_date") or get_run_date(spark)

# COMMAND ----------
# MAGIC %md #### 2. Init Logger

# COMMAND ----------

log = Logger(
    notebook_name=f"gold_{TARGET_TABLE}",
    area=TARGET_SCHEMA,
    layer="gold",
)
log.log_run_start(
    source_table=f"{SILVER_CATALOG}.{SILVER_SCHEMA}.{SILVER_FACT_TABLE}",
    target_table=f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}",
    run_date=RUN_DATE,
)
start_time = time.time()

# COMMAND ----------
# MAGIC %md #### 3. Lettura Silver (solo partizione della run_date)

# COMMAND ----------

silver_fqn = f"{SILVER_CATALOG}.{SILVER_SCHEMA}.{SILVER_FACT_TABLE}"
silver_df = (
    spark.read.format("delta")
    .table(silver_fqn)
    .filter(F.col(PARTITION_COL).cast("date") == F.lit(RUN_DATE))
)

rows_read = silver_df.count()
log.info("Silver letto", table=silver_fqn, partition=RUN_DATE, rows=rows_read)

if rows_read == 0:
    log.info(f"Nessuna riga Silver per la data {RUN_DATE}. Run terminato.")
    dbutils.notebook.exit("NO_DATA_FOR_DATE")

# COMMAND ----------
# MAGIC %md #### 4. Lettura dimensioni

# COMMAND ----------

def read_dim(table: str) -> "DataFrame":
    """Helper per leggere una dimensione dal catalogo condiviso."""
    return spark.read.format("delta").table(f"{DIM_CATALOG}.{DIM_SCHEMA}.{table}")

dim_fornitore = read_dim("dim_fornitore")
dim_articolo = read_dim("dim_articolo")
dim_calendario = read_dim("dim_calendario")

log.info("Dimensioni caricate", dims=["dim_fornitore", "dim_articolo", "dim_calendario"])

# COMMAND ----------
# MAGIC %md #### 5. Risoluzione surrogate keys con fallback Late-Arriving Dimensions

# COMMAND ----------

# FORNITORE_ID (NK) → FORNITORE_SK (surrogate key)
fact_df = surrogate_key_fallback(
    df=silver_df,
    fk_col="FORNITORE_ID",
    dim_df=dim_fornitore,
    dim_pk="FORNITORE_ID",
    default_val=-1,
)

# ART_ID (NK) → ART_SK (surrogate key)
fact_df = surrogate_key_fallback(
    df=fact_df,
    fk_col="ART_ID",
    dim_df=dim_articolo,
    dim_pk="ART_ID",
    default_val=-1,
)

# DATA_CARICO → DATA_SK (da DIM_CALENDARIO)
fact_df = surrogate_key_fallback(
    df=fact_df,
    fk_col=PARTITION_COL,
    dim_df=dim_calendario.select("DATA_CALENDARIO"),
    dim_pk="DATA_CALENDARIO",
    default_val=None,
)

log.info("Surrogate keys risolti con fallback -1 per Late-Arriving Dimensions")

# COMMAND ----------
# MAGIC %md #### 6. Cast misure decimali

# COMMAND ----------

measure_cols = [c for c in ["PESO_NETTO", "PESO_LORDO", "QTA_RICEVUTA", "IMPORTO_FATTURA"]
                if c in fact_df.columns]
if measure_cols:
    fact_df = cast_decimal(fact_df, measure_cols, precision=18, scale=4)

# COMMAND ----------
# MAGIC %md #### 7. Calcolo misure derivate

# COMMAND ----------

if "PESO_NETTO" in fact_df.columns and "PESO_LORDO" in fact_df.columns:
    fact_df = fact_df.withColumn(
        "TARA",
        F.col("PESO_LORDO") - F.col("PESO_NETTO"),
    )

if "IMPORTO_FATTURA" in fact_df.columns and "QTA_RICEVUTA" in fact_df.columns:
    fact_df = fact_df.withColumn(
        "PREZZO_UNITARIO",
        F.when(F.col("QTA_RICEVUTA") > 0, F.col("IMPORTO_FATTURA") / F.col("QTA_RICEVUTA"))
        .otherwise(F.lit(None).cast("decimal(18,4)")),
    )

# Aggiungi colonna partizione esplicita per replaceWhere
fact_df = fact_df.withColumn("DATA_RIFERIMENTO", F.lit(RUN_DATE).cast("date"))

# COMMAND ----------
# MAGIC %md #### 8. Scrittura Gold con replaceWhere

# COMMAND ----------

dh = DeltaHelper(spark, catalog=TARGET_CATALOG, schema=TARGET_SCHEMA)
dh.replace_where(
    target_table=TARGET_TABLE,
    source_df=fact_df,
    partition_col="DATA_RIFERIMENTO",
    partition_value=RUN_DATE,
)

rows_written = fact_df.count()
log.info(
    "replace_where Gold completato",
    target=f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}",
    partition=RUN_DATE,
    rows_written=rows_written,
)

# COMMAND ----------
# MAGIC %md #### 9. Data Quality post-scrittura

# COMMAND ----------

gold_df = (
    spark.read.format("delta")
    .table(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}")
    .filter(F.col("DATA_RIFERIMENTO") == F.lit(RUN_DATE))
)

dq = DQHelper(
    spark=spark,
    df=gold_df,
    table_name=f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}",
    logger=log,
)

dq_report = dq.run_all([
    ("check_no_nulls", {"cols": ["FORNITORE_ID", "ART_ID"]}),
    ("check_numeric_range", {"col": "PESO_NETTO", "min_val": 0}),
    ("check_row_count", {"expected_min": 1}),
])

if not dq_report["all_passed"]:
    log.error("DQ Gold fallito", failed_count=dq_report["failed_count"])

# COMMAND ----------
# MAGIC %md #### 10. Log finale

# COMMAND ----------

duration = time.time() - start_time
log.log_run_end(
    rows_read=rows_read,
    rows_written=rows_written,
    duration_seconds=duration,
)

dbutils.notebook.exit("SUCCESS")
