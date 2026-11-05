# Databricks notebook source
# Area: Carichi — Fact carico (grain ETICHETTA)
# Layer: Silver PREP (Fase 1 modellazione + Fase 2 calcolo)  →  silver.logistica_curated.carico
# Versione: 3.4.0
# Data: 2026-11-05
#   3.4.0 (ACT_9038): fotografia dopo il giro = primo giorno di landing caricato dopo D (lacuna 31/10–04/11/2026).
#   3.3.0 (ACT_9038): ERRART solo per i carichi con data <= ultimo aggiornamento della tabella varianti (copia statica).
#   3.2.0 (ACT_9038): righe/pesate "del giro" = viste tra la fotografia prima e quella dopo il giro legacy;
#          pesate = solo quelle timbrate nel giro del carico; etichette rinumerate dopo il giro riportate al numero
#          letto dal legacy; nessun requisito sulla prima vista della testata.
#   3.0.0 (ACT_9038): riscritta come CDT_ESTR.V_CARICO_ORDINARIO sullo STATO DEL CARICO AL GIRO LEGACY
#          (spec DOCS/main/spec_legacy_f_carico.md). Superate le correzioni puntuali ACT_9033 (testata per
#          carico), ACT_9035 (riconciliazione bolla/ordine), ACT_9037 (filtro snapshot placeholder).
#   2.4.0 (ACT_9039, ADR-0029): rimosse QTA_ORD_FORN e VAL_COSTO_CARICO (non misure del carico).
#   2.3.0 (ACT_9037) · 2.2.0 (ACT_9035) · 2.1.0 (ACT_9033): vedi storia git.
# Descrizione: STRATO PREP — F_CARICO come la produce il legacy.
#
#   GRAIN = una riga per accoppiamento pesata × gruppo-righe (di fatto etichetta × bolla), come la vista legacy.
#
#   STATO AL GIRO LEGACY (spec R-1.x, R-2.3; parametro legacy_parity=true):
#     il legacy elabora un carico una volta sola, nel giro in cui ne timbra la testata (D = giorno di
#     STCAR_DATA_ESTRAZIONE_DWH), leggendo i record pendenti in quel momento. DataRelay estrae prima del giro,
#     quindi lo stato letto dal legacy = righe/pesate presenti nell'ultima landing S <= D in cui c'è la testata:
#     _first_seen_date <= S <= _last_seen_date (colonne del bronze). Esclude da sé le etichette rinumerate o
#     create dopo il consumo, le pesate cancellate prima del consumo e gli snapshot con bolla placeholder.
#     Carichi non ancora timbrati: esclusi (il legacy non li ha ancora). legacy_parity=false (dopo lo
#     spegnimento del legacy): stato più recente, nessun requisito di timbro.
#
#   MODELLAZIONE (= V_CARICO_ORDINARIO, spec R-2.1/R-2.2):
#     r = righe presenti aggregate per (SITO,CARICO,DATA,ORDINE,MSI,RADICE,VAR,BOLLA): MAX imballi/ora/date,
#         MIN ricevitore.   t ⋈ r su (SITO, CARICO, DATA, ORDINE)   p ⋈ r su (SITO, CARICO, BOLLA, ORDINE, MSI)
#     [MSI = radice+variante; PSP_ARTEAN13 è un MSI]. Tutte INNER, nessuna riconciliazione.
#   SCARTI CDT_SA (spec R-4.x): ERRART 210103 = nessuna versione della variante valida alla data del carico
#     in CDT_DW.L_ART_VAR_LOGIS_ST (widget art_storicizzata; se assente: avviso e nessuno scarto);
#     DOPPI 210101 = righe ripetute su (SITO, DATA, DOC, ETICHETTA, BOLLA, RADICE, VARIANTE) → tutte scartate.
#
#   CALCOLO (spec R-2.4): QTA_UF_CARICO = UF rilevata; QTA_CARICO = colli × pz/cartone; NUM_IMB_CARICO = colli;
#     NUM_IMB_FORN_CARICO = QTA_CARICO / pz imballo fornitore (0 → 1); NUM_PLT_CARICO = 1;
#     STESSO_IMB_CARICO_FLAG = (pz imballo fornitore == pz imballo sito); NUM_BOLLA_FORN = NVL(bolla,'0').
#   PES/VOL nel Gold (LU_ART_UNITA_LOGISTICA). VAL_COSTO_CARICO e QTA_ORD_FORN non esposte (ADR-0029).
#   Dimensione articolo: ART_RADICE_COD + ART_VAR_LOGIS_COD, niente ART_COD né colonne storicizzate.
#
#   SORGENTI (silver.clean): carico_testata, carico_dettaglio, pesata (+ _first/_last_seen_date dal bronze).
#   MODE: overwrite dinamico per ANNO_MESE (ANNO_MESE = mese della DATA_CARICO della testata).

# COMMAND ----------

# MAGIC %pip install /Volumes/landing_dev/logistica/files/_wheels/logistica_utils-1.0.0-py3-none-any.whl

# COMMAND ----------

import sys
import importlib.util as _ilu; sys.path.insert(0, _ilu.find_spec("logistica_utils").submodule_search_locations[0] if _ilu.find_spec("logistica_utils") else "/Workspace/Repos/logistico/logistica_utils")  # wheel: dir del package; fallback locale/Repos

from logging_helper import get_logger
from dq_helper import check_row_count
from utils import get_catalog
from pyspark.sql import functions as F, Window
from pyspark.sql.types import IntegerType, DoubleType, StringType, DateType
from datetime import date

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("run_date", str(date.today()), "Run Date (YYYY-MM-DD)")
dbutils.widgets.dropdown("legacy_parity", "true", ["true", "false"], "Stato al giro legacy (ACT_9038)")
dbutils.widgets.text("art_storicizzata", "", "Tabella L_ART_VAR_LOGIS_ST (vuoto = default)")

env           = dbutils.widgets.get("env")
run_date      = dbutils.widgets.get("run_date")
LEGACY_PARITY = dbutils.widgets.get("legacy_parity") == "true"
dbutils.widgets.text("diagnosi_carico", "", "Diagnosi: SITO_COD|CARICO_NRO (vuoto = esecuzione normale)")
DIAGNOSI = dbutils.widgets.get("diagnosi_carico").strip()

# COMMAND ----------

NOTEBOOK_NAME  = "silver_prep_carico"
SILVER_CATALOG = get_catalog("silver", env)
SCHEMA         = "logistica"
SRC_TESTATA    = f"{SILVER_CATALOG}.{SCHEMA}.carico_testata"
SRC_DETTAGLIO  = f"{SILVER_CATALOG}.{SCHEMA}.carico_dettaglio"
SRC_PESATA     = f"{SILVER_CATALOG}.{SCHEMA}.pesata"
TARGET_TABLE   = f"{SILVER_CATALOG}.logistica_curated.carico"
ART_ST_TABLE   = (dbutils.widgets.get("art_storicizzata").strip()
                  or f"{get_catalog('bronze', env)}.condiviso.l_art_var_logis_st")

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

    yyyymmdd = lambda c: F.date_format(c, "yyyyMMdd").cast("int")
    df_t = (spark.read.table(SRC_TESTATA)
            .filter(F.date_format(F.col("DATA_CARICO"), "yyyyMM") == F.lit(ANNO_MESE)))
    df_d = spark.read.table(SRC_DETTAGLIO)
    df_p = spark.read.table(SRC_PESATA)
    diag = []
    if DIAGNOSI:  # un solo carico, conteggi a ogni passaggio, nessuna scrittura
        _ds, _dc = DIAGNOSI.split("|")
        df_t = df_t.filter((F.col("SITO_COD") == _ds) & (F.col("CARICO_NRO") == _dc))
        df_d = df_d.filter((F.col("SITO_COD") == _ds) & (F.col("CARICO_NRO") == _dc))
        df_p = df_p.filter((F.col("SITO_COD") == _ds) & (F.col("CARICO_LOG_NRO") == _dc))
        diag += [("testate", df_t.count()), ("righe", df_d.count()), ("pesate", df_p.count())]

    # ── stato al giro legacy ──────────────────────────────────────────────────────────────────────────
    #    D = giorno del timbro della testata: il legacy elabora il carico in quel giro (~06:45) e timbra le pesate
    #    che legge. Le nostre landing sono fotografie fatte a orari diversi (prima o dopo il giro, a seconda del
    #    periodo e dell'estrattore), quindi una riga è "del giro" se l'abbiamo vista in un qualsiasi file tra la
    #    fotografia prima del giro (S = min(D, ultima vista testata)) e quella dopo (P = prima landing dopo D in cui
    #    c'è la testata): _first_seen_date <= P e _last_seen_date >= S. Per le pesate decide il TIMBRO: il legacy
    #    timbra esattamente le pesate che legge → si tengono quelle timbrate nel giro del carico (esclude bolle
    #    provvisorie, pesate cancellate, pesate arrivate dopo). Testate non ancora timbrate: escluse.
    stamp_day = lambda c: (lambda s_: F.when(F.length(s_) >= 12, F.to_date(F.substring(s_, 1, 8), "yyyyMMdd")))(
        F.regexp_replace(F.col(c).cast("string"), r"\.0+$", ""))
    df_t = df_t.withColumn("_D", stamp_day("STCAR_DATA_ESTRAZIONE_DWH"))
    if LEGACY_PARITY:
        # la fotografia dopo il giro è il primo giorno di landing davvero caricato dopo D (non D+1 se mancano giorni);
        # le righe integrate da Oracle non sono fotografie: prima della prima landing (settembre) resta D+1
        def _giorni_landing(tab):
            x = spark.read.table(tab)
            return (x.filter(~F.coalesce(F.col("_source_file"), F.lit("")).startswith("oracle"))
                     .select(F.col("_first_seen_date").alias("_g")))
        giorni = _giorni_landing(SRC_TESTATA).unionByName(_giorni_landing(SRC_PESATA)).distinct()
        prima_landing = giorni.agg(F.min("_g")).first()[0]
        dopo = (df_t.select("_D").distinct().join(giorni, F.col("_g") > F.col("_D"))
                    .groupBy("_D").agg(F.min("_g").alias("_N")))
        d1 = F.date_add(F.col("_D"), 1)
        succ = F.when(F.col("_D") >= F.lit(prima_landing), F.coalesce(F.col("_N"), d1)).otherwise(d1)
        df_t = (df_t.filter(F.col("_D").isNotNull()).join(dopo, "_D", "left")
                    .withColumn("_S", F.least(F.col("_D"), F.col("_last_seen_date")))
                    .withColumn("_P", F.least(F.greatest(succ, F.col("_first_seen_date")), F.col("_last_seen_date")))
                    .drop("_N"))
    else:
        df_t = df_t.withColumn("_S", F.col("_last_seen_date")).withColumn("_P", F.col("_last_seen_date"))
    nel_giro = lambda a: (F.col(f"{a}._first_seen_date") <= F.col("_P")) & (F.col(f"{a}._last_seen_date") >= F.col("_S"))
    TK = ["SITO_COD", "CARICO_NRO", "DATA_CARICO", "ORDINE_NRO"]
    t_s = df_t.select(*TK, "_S", "_P")

    # ── (r) righe del giro, aggregate come la subquery "r" di V_CARICO_ORDINARIO ──
    r = (df_d.alias("d").join(t_s, TK, "inner").filter(nel_giro("d"))
         .withColumn("BOLLA_FORN_NRO", F.coalesce(F.col("BOLLA_FORN_NRO").cast(StringType()), F.lit("0")))
         .groupBy(*TK, "MSI_COD", "ART_RADICE", "ART_VAR", "BOLLA_FORN_NRO").agg(
            F.max("ORA_CARICO").alias("ORA_CARICO"),
            F.min("RICEVITORE_COD").alias("RICEVITORE_COD"),
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

    # ── (p) pesate lette dal legacy: del giro e timbrate nel giro del loro carico ──
    s_carico = (df_t.groupBy(F.col("SITO_COD").alias("_sc_sito"), F.col("CARICO_NRO").alias("_sc_carico"))
                .agg(F.max("_D").alias("_D"), F.min("_S").alias("_S"), F.max("_P").alias("_P")))
    # per le pesate decide il timbro: basta che esistessero ancora alla fotografia prima del giro (_last_seen >= S);
    # la prima vista può essere successiva al giro (l'estrattore le mostrava in ritardo, dedup per merge_keys)
    pc = (df_p.alias("pp")
          .join(s_carico, (F.col("pp.SITO_COD") == F.col("_sc_sito")) & (F.col("pp.CARICO_LOG_NRO") == F.col("_sc_carico")), "inner")
          .filter(F.col("pp._last_seen_date") >= F.col("_S"))
          .withColumn("_pD", stamp_day("pp.PSP_DATA_ESTRAZIONE_DWH")))
    pcols = df_p.columns
    if LEGACY_PARITY:
        p_lette = pc.filter(F.col("_pD") == F.col("_D"))
        # rinumerata dopo il giro con il timbro già presente: due pesate timbrate identiche (sito, carico, MSI, colli,
        # UF, bolla) con etichetta diversa che non sono mai esistite insieme (la seconda compare dopo che la prima è
        # sparita) sono la stessa pesata → si tiene la prima, quella letta dal legacy.
        TW = ["SITO_COD", "CARICO_LOG_NRO", "ART_EAN13", "NRO_COLLI", "QTA_UF_RILEVATA", "BOLLA_NRO"]
        prime = p_lette.select(*[F.col(f"pp.{k}").alias(f"_tw_{k}") for k in TW],
                               F.col("pp.ETICHET_NRO").alias("_tw_etic"), F.col("pp._last_seen_date").alias("_tw_last"))
        p_lette = (p_lette.join(prime, F.expr(" AND ".join(f"pp.{k} <=> _tw_{k}" for k in TW)) &
                                (F.col("pp.ETICHET_NRO") != F.col("_tw_etic")) &
                                (F.col("pp._first_seen_date") > F.col("_tw_last")), "left_anti"))
        # etichette rinumerate dopo il giro: il legacy ha letto il numero VECCHIO. Nuova = timbrata nel giro ma vista
        # per la prima volta dopo la fotografia prima del giro; vecchia = mai timbrata, vista prima del giro e
        # sparita dopo. Stessa pesata = stesso sito, carico, MSI, colli, UF → coppie 1:1 per ordine di etichetta.
        PK = ["SITO_COD", "CARICO_LOG_NRO", "ART_EAN13", "NRO_COLLI", "QTA_UF_RILEVATA"]
        nuove = (p_lette.filter(F.col("pp._first_seen_date") > F.col("_S")).select(*[F.col(f"pp.{c}") for c in pcols])
                 .withColumn("_rk", F.row_number().over(Window.partitionBy(*PK).orderBy("ETICHET_NRO"))))
        # NB: se la stessa etichetta è tra le timbrate non è una rinumerazione ma una correzione (es. bolla '1' → reale
        # prima del giro): il legacy ha letto la versione corretta → la vecchia non si usa.
        etic_lette = p_lette.select(F.col("pp.SITO_COD").alias("_l_sito"), F.col("pp.CARICO_LOG_NRO").alias("_l_car"),
                                    F.col("pp.ETICHET_NRO").alias("_l_etic")).distinct()
        vecchie = (pc.filter(F.col("_pD").isNull() & (F.col("pp._first_seen_date") <= F.col("_S")) &
                             (F.col("pp._last_seen_date") < F.col("_P")))
                   .select(*[F.col(f"pp.{c}") for c in pcols])
                   .join(etic_lette, (F.col("SITO_COD") == F.col("_l_sito")) & (F.col("CARICO_LOG_NRO") == F.col("_l_car")) &
                         (F.col("ETICHET_NRO") == F.col("_l_etic")), "left_anti")
                   .withColumn("_rk", F.row_number().over(Window.partitionBy(*PK).orderBy("ETICHET_NRO"))))
        coppie = nuove.alias("n").join(vecchie.alias("v"), PK + ["_rk"], "inner")
        sostituite = coppie.select(F.col("n.ETICHET_NRO").alias("_rin_etic"), *[F.col(f"n.{k}").alias(f"_rin_{k}") for k in PK])
        logger.info(f"etichette rinumerate dopo il giro (riportate al numero letto dal legacy): {coppie.count()}")
        p_base = (p_lette.select(*[F.col(f"pp.{c}") for c in pcols]).alias("b")
                  .join(sostituite, (F.col("b.ETICHET_NRO") == F.col("_rin_etic")) &
                        F.expr(" AND ".join(f"b.{k} <=> _rin_{k}" for k in PK)), "left_anti"))
        p = p_base.unionByName(coppie.select(*[F.col(f"v.{c}") for c in pcols]))
        if DIAGNOSI:
            diag += [("testate timbrate", df_t.count()), ("righe del giro (gruppi r)", r.count()),
                     ("pesate del giro", pc.count()), ("pesate timbrate nel giro", p_lette.count()),
                     ("coppie rinumerate", coppie.count()), ("pesate finali", p.count())]
    else:
        p = pc.select(*[F.col(f"pp.{c}") for c in pcols])

    # ── V_CARICO_ORDINARIO: p ⋈ r (sito, carico, bolla, ordine, MSI) ⋈ t (sito, carico, data, ordine) ──
    j = (p.alias("p")
         .join(r.alias("r"),
               (F.col("p.SITO_COD")       == F.col("r.SITO_COD")) &
               (F.col("p.CARICO_LOG_NRO") == F.col("r.CARICO_NRO")) &
               (F.col("p.BOLLA_NRO")      == F.col("r.BOLLA_FORN_NRO")) &
               (F.col("p.COMMESSA_NRO")   == F.col("r.ORDINE_NRO")) &
               (F.col("p.ART_EAN13")      == F.col("r.MSI_COD")), "inner")
         .join(df_t.alias("t"),
               (F.col("r.SITO_COD") == F.col("t.SITO_COD")) & (F.col("r.CARICO_NRO") == F.col("t.CARICO_NRO")) &
               (F.col("r.DATA_CARICO") == F.col("t.DATA_CARICO")) & (F.col("r.ORDINE_NRO") == F.col("t.ORDINE_NRO")),
               "inner"))

    # ── scarto ERRART (210103): variante non valida alla data del carico nell'anagrafica storicizzata ──
    if spark.catalog.tableExists(ART_ST_TABLE):
        st = (spark.read.table(ART_ST_TABLE)
              .select(F.col("ART_RADICE_COD").cast(StringType()).alias("_st_rad"),
                      F.col("ART_VAR_LOGIS_COD").cast("int").alias("_st_var"),
                      F.col("ART_ST_DATA_INIZ").cast("int").alias("_st_ini"),
                      F.col("ART_ST_DATA_FINE").cast("int").alias("_st_fin")))
        # copia statica: le varianti nate dopo l'ultimo aggiornamento non ci sono → lo scarto vale solo fino a quel giorno
        art_agg = spark.sql(f"DESCRIBE DETAIL {ART_ST_TABLE}").first()["lastModified"].date()
        n0 = j.count()
        k = (j.select(F.col("r.ART_RADICE").cast(StringType()).alias("_k_rad"), F.col("r.ART_VAR").cast("int").alias("_k_var"),
                      F.col("r.DATA_CARICO").alias("_k_dt")).distinct())
        art_ok = (k.join(st, (F.col("_k_rad") == F.col("_st_rad")) & (F.col("_k_var") == F.col("_st_var")) &
                             yyyymmdd(F.col("_k_dt")).between(F.col("_st_ini"), F.col("_st_fin")), "left_semi")
                   .withColumn("_art_ok", F.lit(True)))
        j = (j.join(art_ok, (F.col("r.ART_RADICE").cast(StringType()) == F.col("_k_rad")) &
                            (F.col("r.ART_VAR").cast("int") == F.col("_k_var")) & (F.col("r.DATA_CARICO") == F.col("_k_dt")), "left")
              .filter(F.col("_art_ok").isNotNull() | (F.col("r.DATA_CARICO") > F.lit(art_agg)))
              .drop("_k_rad", "_k_var", "_k_dt", "_art_ok"))
        logger.info(f"ERRART: scartate {n0 - j.count()} righe (variante non valida alla data in {ART_ST_TABLE}, "
                    f"applicato ai carichi fino al {art_agg})")
    else:
        logger.warning(f"ERRART non applicato: {ART_ST_TABLE} assente")

    # ── scarto DOPPI (210101): righe ripetute sulla chiave di V_DOPPI_T_CARICO → escluse tutte ──
    _wd = Window.partitionBy(F.col("r.SITO_COD"), F.col("r.DATA_CARICO"), F.col("r.CARICO_NRO"), F.col("p.ETICHET_NRO"),
                             F.col("r.BOLLA_FORN_NRO"), F.col("r.ART_RADICE"), F.col("r.ART_VAR"))
    j = j.withColumn("_n_doppi", F.count(F.lit(1)).over(_wd))
    logger.info(f"DOPPI: scartate {j.filter(F.col('_n_doppi') > 1).count()} righe")
    if DIAGNOSI:
        diag += [("dopo join e ERRART", j.count()), ("doppi", j.filter(F.col("_n_doppi") > 1).count())]
        dbutils.notebook.exit(str(diag))
    j = j.filter(F.col("_n_doppi") == 1).drop("_n_doppi")

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
            F.col("t.VETTORE_COD").cast(StringType()).alias("VETTORE_COD"),        # vettore carico
            # Date (in CDT_DW sono FK GIORNO_*_ID)
            F.col("t.DATA_EMISSIONE_ORD").cast(DateType()).alias("DATA_EMISSIONE_ORD"),
            F.col("t.DATA_CONFERMA_ORD").cast(DateType()).alias("DATA_PREV_CONS_FORN"),
            F.col("r.DATA_BOLLA_FORN").cast(DateType()).alias("DATA_BOLLA_FORN"),
            F.col("p.DATA_SCADENZA").cast(DateType()).alias("DATA_SCAD_CARICO"),
            # Chiavi documento
            F.col("r.CARICO_NRO").cast(StringType()).alias("NUM_DOC_CARICO"),
            F.col("r.BOLLA_FORN_NRO").cast(StringType()).alias("NUM_BOLLA_FORN"),   # = PSP_NUMBOL (join), NVL '0'
            F.col("r.ORDINE_NRO").cast(StringType()).alias("NUM_ORD_FORN"),
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
            # ⚠️ QTA_CARICO UNITÀ = PEZZI (NRO_COLLI × PZ_PER_CARTONE). Nome ODI-standard (quadratura CDT_DW).
            F.col("QTA_CARICO"),
            F.col("NUM_IMB_CARICO"),
            F.col("NUM_IMB_FORN_CARICO"),
            F.col("NUM_PLT_CARICO"),
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
        # Run successivi: dynamic partition overwrite (solo ANNO_MESE corrente).
        # Opzione del writer Delta: su serverless (Spark Connect) `spark.conf.set` di
        # spark.sql.sources.partitionOverwriteMode e' vietata -> si usa l'option del writer.
        (prep_df.write.format("delta").mode("overwrite")
            .option("partitionOverwriteMode", "dynamic")
            .partitionBy("ANNO_MESE")
            .saveAsTable(TARGET_TABLE))
        logger.info(f"OVERWRITE (dyn) {TARGET_TABLE} ({rows_out} righe)")

    logger.info(f"END {NOTEBOOK_NAME} | righe={rows_out}")

except Exception as e:
    logger.error(f"ERRORE in {NOTEBOOK_NAME}: {str(e)}", exc_info=True)
    raise
