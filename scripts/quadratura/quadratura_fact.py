"""
Quadratura parametrica fact: CDT_DW (ODI legacy) vs Gold Delta (nuovo flusso Spark).

Generalizza quadratura_f_carico.py a più fact via config FACTS. Certifica che il nuovo
flusso produca risultati coerenti col vecchio DWH Oracle, sullo stesso grain di confronto.

FACT SUPPORTATI:
    CARICO         -> CDT_DW.F_CARICO             vs gold_dev_logistica.db/f_carico
    PREP_SPED      -> CDT_DW.F_PREP_SPED          vs gold_dev_logistica.db/f_prep_sped
    GIACENZE       -> CDT_DW.F_STOCK              vs gold_dev_logistica.db/f_giacenze_daily
    TRASPORTO      -> CDT_DW.F_TRASP_MTV          vs gold_dev_logistica.db/f_trasporto
    TURNO_PREP_SITO-> CDT_DW.F_TURNO_PREP_SITO    vs gold_dev_logistica.db/f_turno_prep_sito
    TRACCIABILITA  -> CDT_DW.F_TRACC              vs gold_dev_logistica.db/f_tracciabilita_lotti

GRAIN DI CONFRONTO: (SITO canonico, PERIODO) — PERIODO = giorno (default) o mese (--per-mese).
    Il confronto è aggregato: COUNT righe + SUM misure. Il COUNT è confrontabile solo se il
    grain dei due lati coincide (per F_CARICO: grain etichetta su entrambi dopo il porting).
    Se un fact NON ha dimensione sito (`gold_sito`/`oracle_sito` = None, es. GIACENZE che è per
    MAG_COD e non per sito) il grain si riduce al solo PERIODO e il sito è etichettato "(tutti)".
    Un fact può anche non avere misure comuni confrontabili (`measures` vuote): in quel caso si
    quadra il solo COUNT (es. TRASPORTO: il nostro Gold non espone KM/COSTO).

⚠️ CONFIG LATO ORACLE DA CONFERMARE: per i fact aggiunti con ACT_9009 (GIACENZE, TRASPORTO,
    TURNO_PREP_SITO, TRACCIABILITA) i nomi colonna CDT_DW sono **ipotesi da verificare** con
    `--discover` (non erano validabili offline). Il lato Gold è invece verificato sul codice dei
    notebook. La config viene validata a runtime: se una colonna non esiste lo script si ferma con
    un messaggio esplicito invece di un ORA-00904 oscuro.

MAPPING SITO: MAG_SITO_COD (CDT_DW, es. 0005C) -> codice canonico (05) via CDT_ESTR.S_LOGISTIX
    (int delle cifre, zero-pad 2). Stesso pattern di quadratura_f_carico.

SEMANTICA DATA (per fact):
    - CARICO:    GIORNO_CARICO_ID è FK surrogate -> JOIN CDT_DW.L_GIORNO per GIORNO_DT.
    - PREP_SPED: GIORNO_BOLLA_SPED_ID è già YYYYMMDD numerico -> nessun join.
      NB: lato Gold prep_sped si confronta su DATA_BOLLA_SPED (stessa semantica bolla).

UTILIZZO:
    python quadratura_fact.py --fact CARICO    --discover
    python quadratura_fact.py --fact CARICO    --da 2026-06-15 --a 2026-06-21 --soglia 5.0
    python quadratura_fact.py --fact PREP_SPED --da 2026-06-15 --a 2026-06-21 --per-mese

CONNESSIONE ORACLE: .env in scripts/landing_simulator/ (stesse credenziali del cdtdw extractor).
DIPENDENZE: pip install oracledb python-dotenv pandas pyarrow
             (dal 2026-08-21 oracledb/python-dotenv sono in docker/local_bronze/requirements.txt)

ESECUZIONE NEL CONTAINER logistico-spark (ACT_9015): servono DUE accortezze, altrimenti
fallisce con "Gold non trovata: C:\PROGETTI\..." oppure KeyError su ORACLE_HOST.
  1) LOGISTICO_DATA=/workspace  -> il default DATA_ROOT e' un path Windows; nel container
     il mount e' /workspace/data, quindi DATA_ROOT deve essere /workspace.
  2) caricare il .env nell'ambiente: `set -a; . <path>/.env; set +a` prima di python.

  MSYS_NO_PATHCONV=1 docker exec -e LOGISTICO_DATA=/workspace logistico-spark sh -c \
    "set -a; . /workspace/code/scripts/landing_simulator/.env; set +a; \
     python -u /workspace/code/scripts/quadratura/quadratura_fact.py --fact CARICO \
     --da 2026-06-09 --a 2026-06-21 --soglia 5.0"

ATTENZIONE INTERPRETAZIONE (OP-QDR-1): CDT_DW ha la storia di produzione completa, il Gold
locale solo i giorni effettivamente ingeriti dalla landing. Quadrare su una finestra piu'
ampia della copertura reale del Gold produce chiavi "solo in ODI" e delta ~100% che NON sono
errori di calcolo. Ricavare la finestra dalla copertura effettiva prima di trarre conclusioni.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from urllib.parse import unquote
from typing import Any

try:
    import oracledb
except ImportError:
    print("[ERROR] manca oracledb. pip install oracledb", file=sys.stderr)
    sys.exit(2)

try:
    from dotenv import load_dotenv
    _ENV_FILE = Path(__file__).parent.parent / "landing_simulator" / ".env"
    if _ENV_FILE.exists():
        load_dotenv(_ENV_FILE)
except ImportError:
    pass

try:
    import pandas as pd
    import pyarrow.parquet as pq
except ImportError:
    print("[ERROR] manca pandas/pyarrow. pip install pandas pyarrow", file=sys.stderr)
    sys.exit(2)

# ──────────────────────────────────────────────────────────────────────────────
# Path warehouse
# ──────────────────────────────────────────────────────────────────────────────
DATA_ROOT = Path(os.environ.get("LOGISTICO_DATA", r"C:\PROGETTI\LOGISTICO_DATA"))
WAREHOUSE = DATA_ROOT / "data" / "warehouse"
GOLD_DB   = WAREHOUSE / "gold_dev_logistica.db"

# Mapping siti (fonte autoritativa siti attivi)
SLOGISTIX_SCHEMA = "CDT_ESTR"
SLOGISTIX_TABLE  = "S_LOGISTIX"

# ──────────────────────────────────────────────────────────────────────────────
# Config per fact
#   oracle_date:
#     {"mode": "l_giorno_fk", "fk": "<col>"}  -> JOIN CDT_DW.L_GIORNO
#     {"mode": "yyyymmdd",    "col": "<col>"} -> colonna numerica YYYYMMDD
#   measures: {chiave_logica: nome_colonna}  (stesse chiavi lato oracle e gold)
#             dict vuoto = si quadra il solo COUNT
#   oracle_sito / gold_sito: None -> fact senza dimensione sito (grain = solo PERIODO)
#   exempt_measures: [chiavi] mostrate nel report ma NON conteggiate come anomalia
#             (divergenze deliberate/note: es. misure non alimentate da noi)
#   oracle_confirmed: False -> nomi colonna CDT_DW da confermare con --discover (ACT_9009)
# ──────────────────────────────────────────────────────────────────────────────
FACTS: dict[str, dict[str, Any]] = {
    "CARICO": {
        "oracle_schema": "CDT_DW",
        "oracle_table":  "F_CARICO",
        "oracle_sito":   "MAG_SITO_COD",
        "oracle_date":   {"mode": "l_giorno_fk", "fk": "GIORNO_CARICO_ID"},
        "oracle_measures": {"QTA": "QTA_CARICO", "QTA_UF": "QTA_UF_CARICO", "PES": "PES_CARICO"},
        "gold_path":     GOLD_DB / "f_carico",
        "gold_sito":     "SITO_COD",
        "gold_date":     "DATA_CARICO",
        "gold_measures": {"QTA": "QTA_CARICO", "QTA_UF": "QTA_UF_CARICO", "PES": "PES_CARICO"},
    },
    "PREP_SPED": {
        "oracle_schema": "CDT_DW",
        "oracle_table":  "F_PREP_SPED",
        "oracle_sito":   "MAG_SITO_COD",
        "oracle_date":   {"mode": "yyyymmdd", "col": "GIORNO_BOLLA_SPED_ID"},
        # Per righe scartate (TIPO_SCAR 09/10): GIORNO_BOLLA_SPED_ID=0, filtro per data prelievo.
        "oracle_date_prel": {"mode": "yyyymmdd", "col": "GIORNO_PREL_INIZ_ID"},
        "oracle_measures": {"QTA_PREP": "QTA_PREP", "VAL_CES": "VAL_PREP_CES", "VAL_VEN": "VAL_PREP_VEN"},
        "gold_path":     GOLD_DB / "f_prep_sped",
        "gold_sito":     "MAG_SITO_COD",
        "gold_date":     "DATA_BOLLA_SPED",
        "gold_date_prel": "DATA_PREL_INIZ",
        "gold_measures": {"QTA_PREP": "QTA_PREP", "VAL_CES": "VAL_PREP_CES", "VAL_VEN": "VAL_PREP_VEN"},
    },

    # ── Fact aggiunti con ACT_9009 ────────────────────────────────────────────
    # Lato Gold: colonne VERIFICATE sul codice dei notebook.
    # Lato CDT_DW: nomi IPOTETICI -> confermare con `--discover` prima di quadrare.

    "GIACENZE": {
        # 07 §1.6: controparte CDT_DW.F_STOCK (SP_LOAD_F_STOCK).
        # ⚠️ Il nostro F_GIACENZE_DAILY è per **MAG_COD**, non per sito (nessun SITO_COD):
        #    grain di confronto ridotto al solo PERIODO (aggregato su tutti i magazzini)
        #    finché non è stabilita la mappatura MAG_COD <-> MAG_SITO_COD lato CDT_DW.
        "oracle_schema": "CDT_DW",
        "oracle_table":  "F_STOCK",
        "oracle_sito":   None,
        "oracle_date":   {"mode": "l_giorno_fk", "fk": "GIORNO_STOCK_ID"},   # da confermare
        "oracle_measures": {},          # nomi misure da confermare con --discover -> per ora solo COUNT
        "gold_path":     GOLD_DB / "f_giacenze_daily",
        "gold_sito":     None,
        "gold_date":     "DATA_FOTO",
        "gold_measures": {},
        # VAL_STOCK_* non alimentati (sorgente cndstostock dismessa: ST-01/OP-CAR-1)
        "exempt_measures": [],
        "oracle_confirmed": False,
        "note": ("Gold per MAG_COD (no sito) -> confronto per sola data. Misure Gold disponibili: "
                 "QTA_PEZZI/QTA_UF/PREZZO_MEDIO_PONDERATO; VAL_STOCK inesistente (ST-01). "
                 "Aggiungere le misure quando i nomi CDT_DW sono confermati via --discover."),
    },

    "TRASPORTO": {
        # 07 §1.3 + ADR-0013: scope grana MTV (F_TRASP_MTV). TRATTA/BOLLA fuori scope.
        # ⚠️ Il nostro F_TRASPORTO NON espone KM né COSTO_EUR (listini corrieri assenti):
        #    la quadratura è sul COUNT dei movimenti; LEAD_TIME_GG è nostro-only (esente).
        "oracle_schema": "CDT_DW",
        "oracle_table":  "F_TRASP_MTV",
        "oracle_sito":   "MAG_SITO_COD",
        "oracle_date":   {"mode": "yyyymmdd", "col": "GIORNO_BOLLA_SPED_ID"},  # da confermare
        "oracle_measures": {},
        "gold_path":     GOLD_DB / "f_trasporto",
        "gold_sito":     "MAG_SITO_COD",
        "gold_date":     "DATA_BOLLA_SPED",
        "gold_measures": {},
        "exempt_measures": [],
        "oracle_confirmed": False,
        "note": ("Solo COUNT: il Gold non ha KM/COSTO_EUR (ADR-0013 scope MTV; listini assenti). "
                 "LEAD_TIME_GG esiste solo lato nostro -> non confrontabile."),
    },

    "TURNO_PREP_SITO": {
        # 07 §1.4: controparte singola CDT_DW.F_TURNO_PREP_SITO, grana coerente.
        # NB: SP_LOAD lavora per mese -> valutare anche `--per-mese`.
        "oracle_schema": "CDT_DW",
        "oracle_table":  "F_TURNO_PREP_SITO",
        "oracle_sito":   "MAG_SITO_COD",
        "oracle_date":   {"mode": "l_giorno_fk", "fk": "GIORNO_PREPARAZ_ID"},  # da confermare
        "oracle_measures": {},          # attesi ORE_LAVORATE/ORE_PRODUTTIVE: confermare i nomi
        "gold_path":     GOLD_DB / "f_turno_prep_sito",
        "gold_sito":     "SITO_COD",
        "gold_date":     "DATA_PREPARAZ",
        "gold_measures": {},
        "exempt_measures": [],
        "oracle_confirmed": False,
        "note": ("Misure Gold disponibili: ORE_LAVORATE, ORE_PRODUTTIVE, NUM_PREPARATI, "
                 "NUM_INEVASI, NUM_REFERENZE. Popolare `*_measures` dopo il --discover."),
    },

    "TRACCIABILITA": {
        # 07 §1.8: controparte CDT_DW.F_TRACC (+ _STEP di staging), grana etichetta CE178.
        "oracle_schema": "CDT_DW",
        "oracle_table":  "F_TRACC",
        "oracle_sito":   "MAG_SITO_COD",
        "oracle_date":   {"mode": "l_giorno_fk", "fk": "GIORNO_CARICO_ID"},   # da confermare
        "oracle_measures": {},
        "gold_path":     GOLD_DB / "f_tracciabilita_lotti",
        "gold_sito":     "SITO_COD",
        "gold_date":     "DATA_CARICO",
        "gold_measures": {},
        "exempt_measures": [],
        "oracle_confirmed": False,
        "note": ("Misure Gold disponibili: NUM_ETICHETTE, NUM_SSCC, NUM_ANNULLATE, "
                 "NUM_TRASFERITE_STAT. Confronto principale = COUNT etichette."),
    },
}


# ──────────────────────────────────────────────────────────────────────────────
# Oracle
# ──────────────────────────────────────────────────────────────────────────────
def connect_oracle() -> "oracledb.Connection":
    dsn = oracledb.makedsn(os.environ["ORACLE_HOST"],
                           int(os.environ.get("ORACLE_PORT", "1521")),
                           service_name=os.environ["ORACLE_SERVICE"])
    conn = oracledb.connect(user=os.environ["ORACLE_USER"],
                            password=os.environ["ORACLE_PASSWORD"], dsn=dsn)
    conn.autocommit = False
    return conn


def _normalize_sito_code(mag_sito_cod: str) -> str | None:
    if not mag_sito_cod:
        return None
    digits = re.sub(r"[^0-9]", "", mag_sito_cod)
    return str(int(digits)).zfill(2) if digits else None


def build_sito_map(conn: "oracledb.Connection") -> dict[str, str]:
    """MAG_SITO_COD (CDT_DW) -> codice canonico Gold, da S_LOGISTIX (siti attivi)."""
    sql = (f"SELECT TRIM(MAG_SITO_COD) FROM {SLOGISTIX_SCHEMA}.{SLOGISTIX_TABLE} "
           f"WHERE FLAG_ATTIVO = 1 AND MAG_SITO_COD IS NOT NULL")
    m: dict[str, str] = {}
    with conn.cursor() as cur:
        cur.execute(sql)
        for (mag,) in cur:
            norm = _normalize_sito_code(mag)
            if norm:
                m[mag.strip().upper()] = norm
    return m


def _oracle_columns(conn: "oracledb.Connection", cfg: dict) -> list[tuple]:
    sql = ("SELECT COLUMN_NAME, DATA_TYPE, NULLABLE FROM ALL_TAB_COLUMNS "
           "WHERE OWNER=:o AND TABLE_NAME=:t ORDER BY COLUMN_ID")
    with conn.cursor() as cur:
        cur.execute(sql, o=cfg["oracle_schema"], t=cfg["oracle_table"])
        return cur.fetchall()


def _configured_oracle_cols(cfg: dict) -> list[str]:
    """Colonne CDT_DW effettivamente usate dalla query (sito opzionale)."""
    used = [c for c in [cfg.get("oracle_sito")] if c]
    used += list(cfg["oracle_measures"].values())
    d = cfg["oracle_date"]
    used.append(d["fk"] if d["mode"] == "l_giorno_fk" else d["col"])
    return used


def discover_columns(conn: "oracledb.Connection", cfg: dict) -> None:
    rows = _oracle_columns(conn, cfg)
    print(f"\n{cfg['oracle_schema']}.{cfg['oracle_table']}: {len(rows)} colonne\n")
    if not rows:
        print("  [!] Tabella non trovata o nessun privilegio di lettura.")
    for name, dtype, nullable in rows:
        print(f"  {name:<34} {dtype:<14} {nullable}")
    names = {r[0] for r in rows}
    print("\nColonne configurate:")
    for c in _configured_oracle_cols(cfg):
        print(f"  {c:<30} {'OK' if c in names else 'NON TROVATA'}")
    if not cfg["oracle_measures"]:
        print("  (nessuna misura configurata: si quadra il solo COUNT)")
    if not cfg.get("oracle_confirmed", True):
        print("\n[!] Config CDT_DW di questo fact e' un'IPOTESI (ACT_9009): confermare i nomi qui sopra\n"
              "    e popolare `oracle_measures`/`gold_measures` in FACTS prima di quadrare i dati.")
    if cfg.get("note"):
        print(f"\nNota: {cfg['note']}")


def validate_config(conn: "oracledb.Connection", cfg: dict, fact: str) -> None:
    """Verifica che le colonne CDT_DW configurate esistano davvero.

    Evita un ORA-00904 oscuro a valle: se manca qualcosa si esce con un messaggio
    che dice cosa manca e come scoprirlo (--discover).
    """
    rows = _oracle_columns(conn, cfg)
    if not rows:
        print(f"[ERROR] {cfg['oracle_schema']}.{cfg['oracle_table']} non trovata (o non leggibile).",
              file=sys.stderr)
        sys.exit(2)
    names = {r[0] for r in rows}
    missing = [c for c in _configured_oracle_cols(cfg) if c not in names]
    if missing:
        print(f"[ERROR] Config fact {fact}: colonne non presenti in "
              f"{cfg['oracle_schema']}.{cfg['oracle_table']}: {', '.join(missing)}\n"
              f"        Esegui `--fact {fact} --discover` e correggi FACTS in questo script.",
              file=sys.stderr)
        sys.exit(2)


NO_SITO = "(tutti)"   # etichetta usata quando il fact non ha dimensione sito


def query_oracle_kpi(conn, cfg, da, a, siti_filter, per_mese, sito_map):
    c_sito = cfg.get("oracle_sito")
    meas = cfg["oracle_measures"]
    d = cfg["oracle_date"]
    bind: dict[str, Any] = {}

    if d["mode"] == "l_giorno_fk":
        from_clause = (f"{cfg['oracle_schema']}.{cfg['oracle_table']} f "
                       f"JOIN CDT_DW.L_GIORNO g ON g.GIORNO_ID = f.{d['fk']}")
        date_expr = "g.GIORNO_DT"
        where_date = "g.GIORNO_DT BETWEEN TO_DATE(:da,'YYYY-MM-DD') AND TO_DATE(:a,'YYYY-MM-DD')"
        bind["da"] = da
        bind["a"]  = a
    else:  # yyyymmdd numerico
        from_clause = f"{cfg['oracle_schema']}.{cfg['oracle_table']} f"
        date_expr = f"TO_DATE(f.{d['col']}, 'YYYYMMDD')"
        where_date = f"f.{d['col']} BETWEEN :da_i AND :a_i"
        bind["da_i"] = int(da.replace("-", ""))
        bind["a_i"]  = int(a.replace("-", ""))

    periodo_expr = (f"TO_CHAR(TRUNC({date_expr},'MM'),'YYYYMM')" if per_mese
                    else f"TO_CHAR(TRUNC({date_expr}),'YYYY-MM-DD')")

    sito_clause = ""
    if siti_filter and c_sito:
        siti_norm = {s.strip().upper() for s in siti_filter}
        mag_cods = [m for m, norm in sito_map.items() if norm in siti_norm]
        if mag_cods:
            ph = ", ".join(f":s{i}" for i in range(len(mag_cods)))
            sito_clause = f"AND TRIM(f.{c_sito}) IN ({ph})"
            bind.update({f"s{i}": s for i, s in enumerate(mag_cods)})

    # Fact senza dimensione sito (es. GIACENZE, per MAG_COD): grain = solo PERIODO.
    sito_select   = f"TRIM(f.{c_sito})" if c_sito else f"'{NO_SITO}'"
    sito_group_by = f"TRIM(f.{c_sito}), " if c_sito else ""

    sum_cols = "".join(f", SUM(NVL(f.{col},0)) AS {k}" for k, col in meas.items())
    sql = f"""
        SELECT {sito_select} AS MAG_SITO_COD, {periodo_expr} AS PERIODO,
               COUNT(*) AS CNT{sum_cols}
        FROM {from_clause}
        WHERE {where_date} {sito_clause}
        GROUP BY {sito_group_by}{periodo_expr}
    """
    result: dict[tuple[str, str], dict[str, float]] = {}
    with conn.cursor() as cur:
        cur.execute(sql, bind)
        col_names = [d0[0] for d0 in cur.description]
        for row in cur:
            rec = dict(zip(col_names, row))
            mag = (rec["MAG_SITO_COD"] or "").strip().upper()
            if c_sito:
                sito = sito_map.get(mag) or _normalize_sito_code(mag) or mag
            else:
                sito = NO_SITO
            key = (sito, str(rec["PERIODO"]))
            agg = result.setdefault(key, {"CNT": 0.0, **{k: 0.0 for k in meas}})
            agg["CNT"] += float(rec["CNT"] or 0)
            for k in meas:
                agg[k] += float(rec[k] or 0.0)
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Gold (lettura diretta parquet, no Spark)
# ──────────────────────────────────────────────────────────────────────────────
def live_delta_files(path: Path) -> list[Path]:
    """Restituisce i SOLI file parquet 'live' della Delta table (add - remove).

    Legge il _delta_log senza Spark: applica l'eventuale checkpoint parquet e poi
    i commit JSON successivi. Fallback: se non c'è _delta_log, tutti i parquet fisici.
    Necessario perché DROP+full_refresh lasciano parquet tombstoned su disco che, se
    letti, gonfiano i conteggi (OP-CAR-4).
    """
    import json
    log = path / "_delta_log"
    if not log.exists():
        return [p for p in path.rglob("*.parquet") if "_delta_log" not in str(p)]

    live: set[str] = set()
    start_version = 0
    # Checkpoint (se presente): _last_checkpoint punta alla versione base
    last_cp = log / "_last_checkpoint"
    if last_cp.exists():
        try:
            cp_ver = json.loads(last_cp.read_text())["version"]
            cp_file = log / f"{cp_ver:020d}.checkpoint.parquet"
            if cp_file.exists():
                cp = pq.ParquetFile(str(cp_file)).read().to_pydict()
                adds = cp.get("add", [])
                for a0 in adds:
                    if a0 and a0.get("path"):
                        live.add(a0["path"])
                start_version = cp_ver + 1
        except Exception:
            live, start_version = set(), 0

    for commit in sorted(log.glob("*.json")):
        try:
            ver = int(commit.stem)
        except ValueError:
            continue
        if ver < start_version:
            continue
        for line in commit.read_text().splitlines():
            if not line.strip():
                continue
            o = json.loads(line)
            if "add" in o:
                live.add(o["add"]["path"])
            elif "remove" in o:
                live.discard(o["remove"]["path"])

    return [path / p for p in live]


def hive_partition_values(file_path: Path, table_root: Path) -> dict[str, str]:
    """Valori delle colonne di partizione ricavati dal path Hive-style (COL=valore/).

    Serve perche' una colonna usata in partitionBy NON e' materializzata dentro il
    parquet: vive solo nel path (es. f_giacenze_daily -> DATA_FOTO,
    f_turno_prep_sito -> DATA_PREPARAZ). Senza questo la colonna risulterebbe tutta
    NULL e il filtro per data scarterebbe ogni riga.
    """
    vals: dict[str, str] = {}
    try:
        rel = file_path.relative_to(table_root)
    except ValueError:
        return vals
    for seg in rel.parts[:-1]:
        if "=" in seg:
            k, _, v = seg.partition("=")
            vals[k] = unquote(v)
    return vals


def read_gold_frame(path: Path, cols: list[str]):
    """Legge le colonne richieste dai soli file 'live' della Delta table.

    Le colonne assenti dal parquet perche' di partizione sono reidratate dal path.
    Restituisce None se non c'e' nulla da leggere.
    """
    frames = []
    for f in live_delta_files(path):
        try:
            # ParquetFile legge il singolo file: nessuna inferenza partizioni dal path
            pf = pq.ParquetFile(str(f))
            names = set(pf.schema_arrow.names)
            file_cols = [c for c in cols if c in names]
            pdf = pf.read(columns=file_cols if file_cols else None).to_pandas().reindex(columns=cols)
            parts = hive_partition_values(f, path)
            for c in cols:
                if c not in names and c in parts:
                    pdf[c] = parts[c]
            frames.append(pdf)
        except Exception:
            pass
    return pd.concat(frames, ignore_index=True) if frames else None


def query_gold_kpi(cfg, da, a, siti_filter, per_mese):
    path = cfg["gold_path"]
    if not path.exists():
        raise FileNotFoundError(f"Gold non trovata: {path}")

    c_sito, c_date, meas = cfg.get("gold_sito"), cfg["gold_date"], cfg["gold_measures"]
    cols = ([c_sito] if c_sito else []) + [c_date] + list(meas.values())

    df = read_gold_frame(path, cols)
    if df is None:
        return {}

    # Normalizzo a datetime64 su entrambi i lati: `.dt.date` produce object e il confronto
    # con datetime.date cambia comportamento tra versioni pandas.
    df[c_date] = pd.to_datetime(df[c_date], errors="coerce").dt.normalize()
    da_dt, a_dt = pd.Timestamp(da).normalize(), pd.Timestamp(a).normalize()
    df = df[(df[c_date] >= da_dt) & (df[c_date] <= a_dt)]
    if siti_filter and c_sito:
        su = {s.upper() for s in siti_filter}
        df = df[df[c_sito].astype(str).str.strip().str.upper().isin(su)]
    if df.empty:
        return {}

    per = pd.to_datetime(df[c_date])
    df["PERIODO"] = per.dt.strftime("%Y%m") if per_mese else per.dt.strftime("%Y-%m-%d")
    # Fact senza dimensione sito: chiave unica NO_SITO (grain = solo PERIODO).
    df["_SITO"] = df[c_sito].astype(str).str.strip().str.upper() if c_sito else NO_SITO
    for k, col in meas.items():
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Il COUNT si appoggia alla data (sempre presente) così da reggere `measures` vuote.
    agg_spec = {"CNT": (c_date, "count")}
    agg_spec.update({k: (col, "sum") for k, col in meas.items()})
    g = df.groupby(["_SITO", "PERIODO"]).agg(**agg_spec).reset_index()

    result: dict[tuple[str, str], dict[str, float]] = {}
    for _, r in g.iterrows():
        key = (str(r["_SITO"]), str(r["PERIODO"]))
        result[key] = {"CNT": float(r["CNT"]), **{k: float(r[k]) for k in meas}}
    return result


# ──────────────────────────────────────────────────────────────────────────────
# OP-PSP-1: righe scartate senza bolla (GIORNO_BOLLA_SPED_ID=0 in CDT_DW,
#           DATA_BOLLA_SPED IS NULL in Gold). TIPO_SCAR_PREP_COD 09/10 — corretto.
# ──────────────────────────────────────────────────────────────────────────────
def query_oracle_no_bolla_kpi(conn, cfg, da, a, siti_filter, sito_map):
    """Conta righe senza bolla (GIORNO_BOLLA_SPED_ID=0) per sito, filtrate per data prelievo."""
    dp = cfg.get("oracle_date_prel")
    if not dp:
        return {}
    c_sito = cfg["oracle_sito"]
    meas   = cfg["oracle_measures"]
    bind: dict[str, Any] = {"da_i": int(da.replace("-", "")), "a_i": int(a.replace("-", ""))}

    sito_clause = ""
    if siti_filter:
        siti_norm = {s.strip().upper() for s in siti_filter}
        mag_cods = [m for m, norm in sito_map.items() if norm in siti_norm]
        if mag_cods:
            ph = ", ".join(f":s{i}" for i in range(len(mag_cods)))
            sito_clause = f"AND TRIM(f.{c_sito}) IN ({ph})"
            bind.update({f"s{i}": s for i, s in enumerate(mag_cods)})

    sum_cols = ", ".join(f"SUM(NVL(f.{col},0)) AS {k}" for k, col in meas.items())
    sql = f"""
        SELECT TRIM(f.{c_sito}) AS MAG_SITO_COD, COUNT(*) AS CNT, {sum_cols}
        FROM {cfg['oracle_schema']}.{cfg['oracle_table']} f
        WHERE f.GIORNO_BOLLA_SPED_ID = 0
          AND f.{dp['col']} BETWEEN :da_i AND :a_i
          {sito_clause}
        GROUP BY TRIM(f.{c_sito})
    """
    result: dict[str, dict[str, float]] = {}
    with conn.cursor() as cur:
        cur.execute(sql, bind)
        col_names = [d0[0] for d0 in cur.description]
        for row in cur:
            rec = dict(zip(col_names, row))
            mag  = (rec["MAG_SITO_COD"] or "").strip().upper()
            sito = sito_map.get(mag) or _normalize_sito_code(mag) or mag
            result[sito] = {"CNT": float(rec["CNT"] or 0),
                            **{k: float(rec[k] or 0.0) for k in meas}}
    return result


def query_gold_no_bolla_kpi(cfg, da, a, siti_filter):
    """Conta righe Gold con DATA_BOLLA_SPED IS NULL filtrate per data prelievo."""
    dp_col = cfg.get("gold_date_prel")
    if not dp_col:
        return {}
    path  = cfg["gold_path"]
    c_sito = cfg["gold_sito"]
    c_bolla = cfg["gold_date"]
    meas  = cfg["gold_measures"]
    cols  = [c_sito, c_bolla, dp_col] + list(meas.values())

    df = read_gold_frame(path, cols)
    if df is None:
        return {}

    df[c_bolla] = pd.to_datetime(df[c_bolla], errors="coerce").dt.normalize()
    df[dp_col]  = pd.to_datetime(df[dp_col],  errors="coerce").dt.normalize()
    da_dt, a_dt = pd.Timestamp(da).normalize(), pd.Timestamp(a).normalize()

    df = df[df[c_bolla].isna() & df[dp_col].between(da_dt, a_dt)]
    if siti_filter:
        su = {s.upper() for s in siti_filter}
        df = df[df[c_sito].astype(str).str.strip().str.upper().isin(su)]
    if df.empty:
        return {}

    df["_SITO"] = df[c_sito].astype(str).str.strip().str.upper()
    for k, col in meas.items():
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    agg_spec = {"CNT": (dp_col, "count")}
    agg_spec.update({k: (col, "sum") for k, col in meas.items()})
    g = df.groupby("_SITO").agg(**agg_spec).reset_index()

    return {str(r["_SITO"]): {"CNT": float(r["CNT"]), **{k: float(r[k]) for k in meas}}
            for _, r in g.iterrows()}


def print_no_bolla_report(fact, oracle_nb, gold_nb, meas_keys, soglia):
    """Stampa confronto righe senza bolla (scartate TIPO_SCAR 09/10)."""
    if fact != "PREP_SPED":
        return 0
    metrics  = ["CNT"] + meas_keys
    all_siti = sorted(set(oracle_nb) | set(gold_nb))
    if not all_siti:
        print("\n[no-bolla] Nessuna riga senza bolla trovata nel periodo.")
        return 0

    print("\n" + "=" * 70)
    print("  BLOCCO SCARTATE (OP-PSP-1) — righe senza bolla (TIPO_SCAR 09/10)")
    print("  CDT_DW: GIORNO_BOLLA_SPED_ID=0  |  Gold: DATA_BOLLA_SPED IS NULL")
    print("=" * 70)
    hdr = f"{'SITO':<8}" + "".join(f" {m+'_d%':>10}" for m in metrics)
    print(hdr)
    print("-" * len(hdr))

    anomalie = 0
    for sito in all_siti:
        o = oracle_nb.get(sito)
        g = gold_nb.get(sito)
        ha = (o is None) or (g is None)
        cells = []
        for m in metrics:
            ov = (o or {}).get(m, 0.0)
            gv = (g or {}).get(m, 0.0)
            p  = _pct(ov, gv)
            if p > soglia:
                ha = True
            cells.append(f"{p:>9.1f}%")
        if ha:
            anomalie += 1
        flag = " !" if ha else "  "
        oc = (o or {}).get("CNT", 0)
        gc = (g or {}).get("CNT", 0)
        print(f"{sito:<8}" + "".join(f" {c}" for c in cells) + flag
              + f"  (ODI {oc:.0f} | Gold {gc:.0f})")
    print(f"\n[no-bolla] {len(all_siti)} siti, {anomalie} anomalie.")
    return anomalie


# ──────────────────────────────────────────────────────────────────────────────
# Report
# ──────────────────────────────────────────────────────────────────────────────
def _pct(ref: float, new: float) -> float:
    if ref == 0 and new == 0:
        return 0.0
    if ref == 0:
        return 100.0
    return abs(new - ref) / abs(ref) * 100.0


def print_report(fact, oracle_kpi, gold_kpi, meas_keys, soglia, per_mese, exempt=()):
    """`exempt`: misure mostrate ma non conteggiate come anomalia (divergenze note)."""
    metrics = ["CNT"] + meas_keys
    all_keys = sorted(set(oracle_kpi) | set(gold_kpi))
    per_lbl = "MESE" if per_mese else "DATA"

    hdr = f"{'SITO':<8} {per_lbl:<10}" + "".join(f" {m+'_d%':>10}" for m in metrics)
    print("\n" + "=" * len(hdr))
    print(f"  QUADRATURA {fact}  —  CDT_DW/ODI vs Gold/Spark  (soglia {soglia:.1f}%)")
    print("=" * len(hdr))
    print(hdr)
    print("-" * len(hdr))

    anomalie = 0
    for key in all_keys:
        sito, per = key
        o = oracle_kpi.get(key)
        g = gold_kpi.get(key)
        ha = (o is None) or (g is None)
        cells = []
        for m in metrics:
            ov = (o or {}).get(m, 0.0)
            gv = (g or {}).get(m, 0.0)
            p = _pct(ov, gv)
            if p > soglia and m not in exempt:
                ha = True
            cells.append(f"{p:>9.1f}%" + ("~" if m in exempt else ""))
        if ha:
            anomalie += 1
        flag = " !" if ha else "  "
        print(f"{sito:<8} {per:<10}" + "".join(f" {c}" for c in cells) + flag)

    only_o = [k for k in oracle_kpi if k not in gold_kpi]
    only_g = [k for k in gold_kpi if k not in oracle_kpi]
    grain = "sito×mese" if per_mese else "sito×giorno"
    if only_o:
        print(f"\n[!] Solo in ODI ({len(only_o)} {grain}): " +
              ", ".join(f"{k[0]}/{k[1]}" for k in only_o[:20]) + (" ..." if len(only_o) > 20 else ""))
    if only_g:
        print(f"[!] Solo in Gold ({len(only_g)} {grain}): " +
              ", ".join(f"{k[0]}/{k[1]}" for k in only_g[:20]) + (" ..." if len(only_g) > 20 else ""))

    if exempt:
        print(f"\n(~) misure escluse dal giudizio (divergenze note/deliberate): {', '.join(exempt)}")
    if not meas_keys:
        print("\n(i) Nessuna misura comune configurata: confronto sul solo COUNT righe.")
    print(f"\nRIEPILOGO {fact}: {len(all_keys)} chiavi {grain}, {anomalie} con anomalie "
          f"(delta > {soglia:.1f}% o chiave mancante).")
    print("OK  Quadratura OK" if anomalie == 0 else "KO  Differenze rilevate")
    return anomalie


# ──────────────────────────────────────────────────────────────────────────────
def main() -> None:
    p = argparse.ArgumentParser(description="Quadratura parametrica CDT_DW vs Gold.")
    p.add_argument("--fact", required=True, choices=sorted(FACTS), help="Fact da quadrare")
    p.add_argument("--discover", action="store_true", help="Mostra colonne CDT_DW ed esce")
    p.add_argument("--da", metavar="YYYY-MM-DD")
    p.add_argument("--a",  metavar="YYYY-MM-DD")
    p.add_argument("--siti", default="", help="Filtro siti canonici (es. 05,09); default tutti")
    p.add_argument("--soglia", type=float, default=1.0)
    p.add_argument("--per-mese", action="store_true")
    args = p.parse_args()

    cfg = FACTS[args.fact]
    if not args.discover and not (args.da and args.a):
        p.error("--da e --a obbligatori (o --discover)")
    for v in (args.da, args.a):
        if v and not re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            p.error("date in formato YYYY-MM-DD")

    siti_filter = [s.strip().upper() for s in args.siti.split(",") if s.strip()] or None

    print("Connessione Oracle...")
    conn = connect_oracle()
    try:
        if args.discover:
            discover_columns(conn, cfg)
            return
        validate_config(conn, cfg, args.fact)
        if not cfg.get("oracle_confirmed", True):
            print(f"[!] Config CDT_DW del fact {args.fact} non ancora confermata (ACT_9009): "
                  f"le colonne esistono ma il mapping va validato. Vedi --discover.")
        if cfg.get("note"):
            print(f"[i] {cfg['note']}")
        if not cfg.get("gold_sito"):
            print(f"[i] Fact senza dimensione sito: confronto per solo {'mese' if args.per_mese else 'giorno'} "
                  f"(etichetta sito '{NO_SITO}'); --siti ignorato.")
        print(f"Fact: {args.fact} | {args.da} -> {args.a} | "
              f"grain: {'mensile' if args.per_mese else 'giornaliero'} | soglia {args.soglia:.1f}%")
        sito_map = build_sito_map(conn) if cfg.get("oracle_sito") else {}
        if cfg.get("oracle_sito"):
            print(f"Siti mappati da S_LOGISTIX: {len(sito_map)}")
        print(f"Query {cfg['oracle_schema']}.{cfg['oracle_table']}...")
        oracle_kpi = query_oracle_kpi(conn, cfg, args.da, args.a, siti_filter, args.per_mese, sito_map)
        print(f"  -> {len(oracle_kpi)} chiavi in CDT_DW")
        oracle_nb: dict = {}
        if args.fact == "PREP_SPED":
            print(f"Query no-bolla (OP-PSP-1)...")
            oracle_nb = query_oracle_no_bolla_kpi(conn, cfg, args.da, args.a, siti_filter, sito_map)
            print(f"  -> {len(oracle_nb)} siti con righe senza bolla in CDT_DW")
    finally:
        conn.close()

    print("Lettura Gold (parquet)...")
    gold_kpi = query_gold_kpi(cfg, args.da, args.a, siti_filter, args.per_mese)
    print(f"  -> {len(gold_kpi)} chiavi in Gold")
    gold_nb: dict = {}
    if args.fact == "PREP_SPED":
        gold_nb = query_gold_no_bolla_kpi(cfg, args.da, args.a, siti_filter)
        print(f"  -> {len(gold_nb)} siti con righe senza bolla in Gold")

    meas_keys = list(cfg["oracle_measures"])
    n  = print_report(args.fact, oracle_kpi, gold_kpi, meas_keys, args.soglia, args.per_mese,
                      exempt=tuple(cfg.get("exempt_measures", ())))
    n += print_no_bolla_report(args.fact, oracle_nb, gold_nb, meas_keys, args.soglia)
    sys.exit(1 if n > 0 else 0)


if __name__ == "__main__":
    main()
