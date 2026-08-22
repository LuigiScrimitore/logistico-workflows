"""
Quadratura F_CARICO: CDT_DW (ODI legacy) vs Gold Delta (nuovo flusso Spark).

Certifica che il nuovo flusso produca risultati coerenti con il vecchio DWH Oracle.

CONFRONTO:
    CDT_DW.F_CARICO         (Oracle, calcolato da ODI)
        vs
    gold_dev_logistica.db/f_carico  (Delta Lake locale, calcolato da Spark)

GRAIN DEL CONFRONTO: (SITO_COD, DATA_CARICO)
KPI: COUNT righe, SUM QTA_RICEVUTA, SUM PESO_NETTO

Il grain giornaliero permette di confrontare solo il sottoinsieme di date
disponibile nella nostra landing (es. dal 2026-06-09 in poi) senza includere
i giorni precedenti presenti solo in CDT_DW.

UTILIZZO:
    # Prima esecuzione: scopri le colonne di CDT_DW.F_CARICO
    python quadratura_f_carico.py --discover

    # Quadratura su un intervallo di date (formato YYYY-MM-DD)
    python quadratura_f_carico.py --da 2026-06-09 --a 2026-06-23

    # Solo alcuni siti, soglia personalizzata
    python quadratura_f_carico.py --da 2026-06-09 --a 2026-06-23 --siti LAIX,LBVX --soglia 2.0

    # Modalità riepilogo mensile invece che giornaliera
    python quadratura_f_carico.py --da 2026-06-09 --a 2026-06-23 --per-mese

CONNESSIONE ORACLE:
    File .env in scripts/landing_simulator/ (gitignored), stesse credenziali del cdtdw extractor:
        ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE, ORACLE_USER, ORACLE_PASSWORD

    L'utente deve vedere CDT_DW (già verificato nell'extractor lookup).

CONFIGURAZIONE COLONNE CDT_DW.F_CARICO:
    Esegui --discover per vedere i nomi reali, poi aggiorna la sezione ORACLE_COLS qui sotto.
    I nomi attuali sono ipotesi basate sul mapping_carichi.md.

DIPENDENZE:
    pip install oracledb python-dotenv pyspark delta-spark
"""

from __future__ import annotations

import argparse
import os
import sys
import re
from pathlib import Path
from typing import Any

try:
    import oracledb
except ImportError:
    print("[ERROR] manca oracledb. Installa con: pip install oracledb", file=sys.stderr)
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
    print("[ERROR] manca pandas/pyarrow. Installa con: pip install pandas pyarrow", file=sys.stderr)
    sys.exit(2)

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURAZIONE COLONNE — aggiorna dopo --discover se i nomi differiscono
# ──────────────────────────────────────────────────────────────────────────────

# Schema e tabella Oracle legacy
ORACLE_SCHEMA = "CDT_DW"
ORACLE_TABLE  = "F_CARICO"

# Tabella statica di mapping siti (fonte autoritativa dei siti attivi Logistix).
# Contiene MAG_SITO_COD nel formato CDT_DW (es. 0005C, 0035A): il codice sito
# canonico del nostro Gold e' int(cifre di MAG_SITO_COD) zero-padded a 2.
SLOGISTIX_SCHEMA = "CDT_ESTR"
SLOGISTIX_TABLE  = "S_LOGISTIX"

# Colonne CDT_DW.F_CARICO (da verificare con --discover)
# Modifica questi nomi se --discover mostra nomi diversi.
ORACLE_COLS = {
    "sito":      "MAG_SITO_COD",    # codice sito / magazzino
    "giorno_fk": "GIORNO_CARICO_ID",# FK a CDT_DW.L_GIORNO (non e' una DATE diretta)
    "qta":       "QTA_CARICO",      # quantita' ricevuta
    "peso":      "PES_CARICO",      # peso carico
}

# Colonne Gold F_CARICO (Delta, nomi reali da schema parquet)
GOLD_COLS = {
    "sito": "SITO_COD",
    "data": "DATA_CARICO",       # tipo DATE nel Silver/Gold
    "qta":  "QTA_UF_RILEVATA",  # quantita' in UF rilevata al carico (= QTA_CARICO in CDT_DW)
    "peso": "PESO_LORDO",        # peso lordo (= PES_CARICO in CDT_DW)
}

# ──────────────────────────────────────────────────────────────────────────────
# Path
# ──────────────────────────────────────────────────────────────────────────────

DATA_ROOT    = Path(os.environ.get("LOGISTICO_DATA", r"C:\PROGETTI\LOGISTICO_DATA"))
WAREHOUSE    = DATA_ROOT / "data" / "warehouse"
GOLD_PATH    = WAREHOUSE / "gold_dev_logistica.db" / "f_carico"


# ──────────────────────────────────────────────────────────────────────────────
# Oracle
# ──────────────────────────────────────────────────────────────────────────────

def connect_oracle() -> "oracledb.Connection":
    host     = os.environ["ORACLE_HOST"]
    port     = int(os.environ.get("ORACLE_PORT", "1521"))
    service  = os.environ["ORACLE_SERVICE"]
    user     = os.environ["ORACLE_USER"]
    password = os.environ["ORACLE_PASSWORD"]
    dsn  = oracledb.makedsn(host, port, service_name=service)
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("ALTER SESSION SET ISOLATION_LEVEL = SERIALIZABLE")
    except oracledb.DatabaseError:
        pass
    conn.autocommit = False
    return conn


def _normalize_sito_code(mag_sito_cod: str) -> str | None:
    """
    Replica la normalizzazione del nostro Silver: estrae le cifre da MAG_SITO_COD,
    le converte a int (rimuove zeri iniziali) e applica zero-padding a 2.
        '0005C' -> '05'   '0035A' -> '35'   '0015B' -> '15'
    Ritorna None se non ci sono cifre.
    """
    if not mag_sito_cod:
        return None
    digits = re.sub(r"[^0-9]", "", mag_sito_cod)
    if not digits:
        return None
    return str(int(digits)).zfill(2)


def build_sito_map(conn: "oracledb.Connection") -> dict[str, str]:
    """
    Legge CDT_ESTR.S_LOGISTIX (siti attivi) e costruisce il mapping
    MAG_SITO_COD (formato CDT_DW) -> SITO_COD canonico (2 cifre, come nel Gold).
    """
    sql = f"""
        SELECT TRIM(MAG_SITO_COD) AS MAG_SITO_COD
        FROM {SLOGISTIX_SCHEMA}.{SLOGISTIX_TABLE}
        WHERE FLAG_ATTIVO = 1 AND MAG_SITO_COD IS NOT NULL
    """
    mapping: dict[str, str] = {}
    with conn.cursor() as cur:
        cur.execute(sql)
        for (mag_cod,) in cur:
            norm = _normalize_sito_code(mag_cod)
            if norm:
                mapping[mag_cod.strip().upper()] = norm
    return mapping


def discover_columns(conn: "oracledb.Connection") -> None:
    """Stampa le colonne di CDT_DW.F_CARICO per configurare ORACLE_COLS."""
    sql = """
        SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = :owner AND TABLE_NAME = :tbl
        ORDER BY COLUMN_ID
    """
    print(f"\nColonne di {ORACLE_SCHEMA}.{ORACLE_TABLE}:\n")
    print(f"  {'COLUMN_NAME':<35} {'DATA_TYPE':<20} {'NULLABLE'}")
    print("  " + "-" * 65)
    with conn.cursor() as cur:
        cur.execute(sql, owner=ORACLE_SCHEMA, tbl=ORACLE_TABLE)
        rows = cur.fetchall()
        if not rows:
            print(f"  [!] Tabella {ORACLE_SCHEMA}.{ORACLE_TABLE} non trovata o non accessibile.")
            print(f"      Verifica che l'utente abbia SELECT su {ORACLE_SCHEMA}.")
        for col_name, dtype, length, nullable in rows:
            print(f"  {col_name:<35} {dtype:<20} {nullable}")

    # Check automatico: verifica che i nomi configurati esistano davvero
    col_names = {r[0] for r in rows}
    print(f"\nNomi attualmente configurati in ORACLE_COLS:")
    for k, v in ORACLE_COLS.items():
        status = "OK" if v in col_names else "NON TROVATA -- aggiorna ORACLE_COLS"
        print(f"  {k:<12} -> {v:<30} {status}")


def query_oracle_kpi(
    conn: "oracledb.Connection",
    da: str,
    a: str,
    siti_filter: list[str] | None,
    per_mese: bool,
    sito_map: dict[str, str],
) -> dict[tuple[str, str], dict[str, Any]]:
    """
    Aggrega KPI da CDT_DW.F_CARICO per (SITO_COD, DATA o ANNO_MESE).

    La data non e' una colonna diretta: e' GIORNO_CARICO_ID (FK a CDT_DW.L_GIORNO).
    Si fa JOIN con L_GIORNO per ottenere GIORNO_DT (DATE).

    Il codice sito grezzo (MAG_SITO_COD, es. '0005C') viene rimappato al codice
    canonico del Gold ('05') via ``sito_map`` costruito da CDT_ESTR.S_LOGISTIX.
    L'aggregazione lato Python somma le eventuali chiavi che collassano sullo
    stesso codice normalizzato.

    da/a: stringhe 'YYYY-MM-DD'.
    per_mese: True -> grain mensile (YYYYMM), False -> giornaliero (YYYY-MM-DD).
    """
    c = ORACLE_COLS

    bind: dict[str, Any] = {"da": da, "a": a}

    # Il filtro siti e' espresso in codici canonici (es. LAIX/05): traduci
    # all'indietro verso i MAG_SITO_COD grezzi che mappano su quei codici.
    sito_clause = ""
    if siti_filter:
        siti_norm = {s.strip().upper() for s in siti_filter}
        mag_cods = [m for m, norm in sito_map.items() if norm in siti_norm]
        if mag_cods:
            s_placeholders = ", ".join(f":s{i}" for i in range(len(mag_cods)))
            sito_clause = f"AND TRIM(f.{c['sito']}) IN ({s_placeholders})"
            bind.update({f"s{i}": s for i, s in enumerate(mag_cods)})

    if per_mese:
        periodo_expr = "TO_CHAR(TRUNC(g.GIORNO_DT,'MM'),'YYYYMM')"
    else:
        periodo_expr = "TO_CHAR(TRUNC(g.GIORNO_DT),'YYYY-MM-DD')"

    sql = f"""
        SELECT
            TRIM(f.{c['sito']})                       AS MAG_SITO_COD,
            {periodo_expr}                            AS PERIODO,
            COUNT(*)                                  AS CNT,
            SUM(NVL(f.{c['qta']},  0))                AS QTA,
            SUM(NVL(f.{c['peso']}, 0))                AS PESO
        FROM {ORACLE_SCHEMA}.{ORACLE_TABLE} f
        JOIN CDT_DW.L_GIORNO g ON g.GIORNO_ID = f.{c['giorno_fk']}
        WHERE g.GIORNO_DT BETWEEN TO_DATE(:da,'YYYY-MM-DD')
                              AND TO_DATE(:a, 'YYYY-MM-DD')
          {sito_clause}
        GROUP BY TRIM(f.{c['sito']}), {periodo_expr}
    """

    result: dict[tuple[str, str], dict[str, Any]] = {}
    with conn.cursor() as cur:
        cur.execute(sql, bind)
        for mag_raw, periodo, cnt, qta, peso in cur:
            mag = (mag_raw or "").strip().upper()
            # rimappa al codice canonico Gold; se assente da S_LOGISTIX prova la
            # normalizzazione diretta (sito attivo non ancora in S_LOGISTIX).
            sito = sito_map.get(mag) or _normalize_sito_code(mag) or mag
            key = (sito, str(periodo))
            agg = result.setdefault(key, {"cnt": 0, "qta": 0.0, "peso": 0.0})
            agg["cnt"]  += int(cnt  or 0)
            agg["qta"]  += float(qta  or 0.0)
            agg["peso"] += float(peso or 0.0)
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Gold (Delta locale — lettura diretta parquet con pandas/pyarrow, no JVM)
# ──────────────────────────────────────────────────────────────────────────────

def query_gold_kpi(
    da: str,
    a: str,
    siti_filter: list[str] | None,
    per_mese: bool,
) -> dict[tuple[str, str], dict[str, Any]]:
    """
    Legge i parquet di Gold F_CARICO direttamente con pyarrow/pandas.
    La tabella e' partizionata per ANNO_MESE — leggiamo solo le partizioni
    che coprono l'intervallo richiesto.
    """
    if not GOLD_PATH.exists():
        raise FileNotFoundError(
            f"Gold F_CARICO non trovata: {GOLD_PATH}\n"
            f"Verifica che il warehouse sia in {DATA_ROOT}"
        )

    c = GOLD_COLS
    da_dt = pd.Timestamp(da).date()
    a_dt  = pd.Timestamp(a).date()

    # Individua le partizioni ANNO_MESE che coprono il range richiesto
    import calendar
    partizioni = []
    cur = da_dt.replace(day=1)
    while cur <= a_dt:
        partizioni.append(cur.strftime("%Y%m"))
        # avanza di un mese
        last_day = calendar.monthrange(cur.year, cur.month)[1]
        import datetime
        cur = cur.replace(day=last_day) + datetime.timedelta(days=1)

    # Leggi solo le partizioni utili (evita di caricare tutta la tabella)
    frames = []
    for anno_mese in partizioni:
        part_path = GOLD_PATH / f"ANNO_MESE={anno_mese}"
        if part_path.exists():
            df_part = pq.read_table(
                str(part_path),
                columns=[c["sito"], c["data"], c["qta"], c["peso"]],
            ).to_pandas()
            df_part["ANNO_MESE"] = anno_mese
            frames.append(df_part)

    if not frames:
        return {}

    df = pd.concat(frames, ignore_index=True)

    # Normalizza colonna data a tipo date
    df[c["data"]] = pd.to_datetime(df[c["data"]]).dt.date

    # Filtra intervallo
    df = df[(df[c["data"]] >= da_dt) & (df[c["data"]] <= a_dt)]

    # Filtro siti
    if siti_filter:
        siti_upper = {s.upper() for s in siti_filter}
        df = df[df[c["sito"]].str.strip().str.upper().isin(siti_upper)]

    if df.empty:
        return {}

    # Colonna periodo
    if per_mese:
        df["PERIODO"] = pd.to_datetime(df[c["data"]]).dt.strftime("%Y%m")
    else:
        df["PERIODO"] = pd.to_datetime(df[c["data"]]).dt.strftime("%Y-%m-%d")

    df[c["sito"]] = df[c["sito"]].str.strip().str.upper()
    df[c["qta"]]  = pd.to_numeric(df[c["qta"]],  errors="coerce").fillna(0.0)
    df[c["peso"]] = pd.to_numeric(df[c["peso"]], errors="coerce").fillna(0.0)

    agg = df.groupby([c["sito"], "PERIODO"]).agg(
        cnt=(c["qta"], "count"),
        qta=(c["qta"], "sum"),
        peso=(c["peso"], "sum"),
    ).reset_index()

    result: dict[tuple[str, str], dict[str, Any]] = {}
    for _, row in agg.iterrows():
        sito = str(row[c["sito"]]).strip().upper()
        result[(sito, str(row["PERIODO"]))] = {
            "cnt":  int(row["cnt"]),
            "qta":  float(row["qta"]),
            "peso": float(row["peso"]),
        }
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Report
# ──────────────────────────────────────────────────────────────────────────────

def _pct(ref: float, new: float) -> float:
    if ref == 0 and new == 0:
        return 0.0
    if ref == 0:
        return 100.0
    return abs(new - ref) / abs(ref) * 100.0


def print_report(
    oracle_kpi: dict[tuple[str, str], dict[str, Any]],
    gold_kpi:   dict[tuple[str, str], dict[str, Any]],
    soglia:     float,
    per_mese:   bool,
) -> int:
    """Stampa il report; restituisce il numero di chiavi con almeno 1 anomalia."""
    all_keys = sorted(set(oracle_kpi) | set(gold_kpi))

    periodo_lbl = "MESE  " if per_mese else "DATA      "
    HDR = (
        f"{'SITO':<8} {periodo_lbl:<10} "
        f"{'ODI_CNT':>10} {'NEW_CNT':>10} {'d_CNT%':>12}  "
        f"{'ODI_QTA':>12} {'NEW_QTA':>12} {'d_QTA%':>12}  "
        f"{'ODI_PESO':>12} {'NEW_PESO':>12} {'d_PESO%':>12}"
    )
    SEP = "-" * len(HDR)

    print()
    print("=" * len(HDR))
    print(f"  QUADRATURA F_CARICO  —  CDT_DW/ODI vs Gold/Spark  (soglia {soglia:.1f}%)")
    print("=" * len(HDR))
    print(HDR)
    print(SEP)

    def fmt(ref, new):
        p = _pct(ref, new)
        flag = " !" if p > soglia else "  "
        return f"{p:>9.2f}%{flag}"

    anomalie = 0
    for key in all_keys:
        sito, periodo = key
        o = oracle_kpi.get(key, {"cnt": 0, "qta": 0.0, "peso": 0.0})
        g = gold_kpi.get(key,   {"cnt": 0, "qta": 0.0, "peso": 0.0})

        ha_anomalia = any(_pct(o[k], g[k]) > soglia for k in ("cnt", "qta", "peso"))
        if key not in oracle_kpi or key not in gold_kpi:
            ha_anomalia = True
        if ha_anomalia:
            anomalie += 1

        print(
            f"{sito:<8} {periodo:<10} "
            f"{o['cnt']:>10,d} {g['cnt']:>10,d} {fmt(o['cnt'],  g['cnt'])}  "
            f"{o['qta']:>12.1f} {g['qta']:>12.1f} {fmt(o['qta'],  g['qta'])}  "
            f"{o['peso']:>12.1f} {g['peso']:>12.1f} {fmt(o['peso'], g['peso'])}"
        )

    print(SEP)

    only_oracle = [k for k in oracle_kpi if k not in gold_kpi]
    only_gold   = [k for k in gold_kpi   if k not in oracle_kpi]

    grain_lbl = "sito×mese" if per_mese else "sito×giorno"

    if only_oracle:
        print(f"\n[!] Presenti solo in ODI ({len(only_oracle)} chiavi {grain_lbl}):")
        for k in only_oracle:
            print(f"    sito={k[0]}  periodo={k[1]}  cnt={oracle_kpi[k]['cnt']:,d}")

    if only_gold:
        print(f"\n[!] Presenti solo in Gold ({len(only_gold)} chiavi {grain_lbl}):")
        for k in only_gold:
            print(f"    sito={k[0]}  periodo={k[1]}  cnt={gold_kpi[k]['cnt']:,d}")

    print(f"\nRIEPILOGO: {len(all_keys)} chiavi {grain_lbl} analizzate, "
          f"{anomalie} con anomalie (delta > {soglia:.1f}% o chiave mancante).")

    if anomalie == 0:
        print("OK  Quadratura OK -- flusso Spark coerente con ODI.")
    else:
        print("KO  Differenze rilevate -- righe marcate con ! richiedono analisi.")

    return anomalie


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(
        description="Quadratura CDT_DW.F_CARICO (ODI) vs gold_f_carico (Spark)."
    )
    p.add_argument("--discover", action="store_true",
                   help="Mostra le colonne di CDT_DW.F_CARICO ed esce")
    p.add_argument("--da", metavar="YYYY-MM-DD",
                   help="Data inizio intervallo (inclusa)")
    p.add_argument("--a",  metavar="YYYY-MM-DD",
                   help="Data fine intervallo (inclusa)")
    p.add_argument("--siti", default="",
                   help="Filtro siti (es. LAIX,LBVX); default: tutti")
    p.add_argument("--soglia", type=float, default=1.0,
                   help="Soglia delta %% per anomalia (default 1.0)")
    p.add_argument("--per-mese", action="store_true",
                   help="Aggrega per mese invece che per giorno")
    args = p.parse_args()

    if not args.discover and not (args.da and args.a):
        p.error("--da e --a sono obbligatori (o usa --discover per esplorare lo schema)")

    if args.da and not re.match(r"^\d{4}-\d{2}-\d{2}$", args.da):
        p.error("--da deve essere in formato YYYY-MM-DD")
    if args.a and not re.match(r"^\d{4}-\d{2}-\d{2}$", args.a):
        p.error("--a deve essere in formato YYYY-MM-DD")

    siti_filter = [s.strip().upper() for s in args.siti.split(",") if s.strip()] \
                  if args.siti else None

    print("Connessione Oracle...")
    conn = connect_oracle()

    try:
        if args.discover:
            discover_columns(conn)
            return

        grain = "mensile" if args.per_mese else "giornaliero"
        print(f"Intervallo: {args.da} -> {args.a}  |  grain: {grain}")
        print(f"Siti: {', '.join(siti_filter) if siti_filter else 'tutti'}")
        print(f"Soglia: {args.soglia:.1f}%")

        print(f"\nMapping siti da {SLOGISTIX_SCHEMA}.{SLOGISTIX_TABLE}...")
        sito_map = build_sito_map(conn)
        print(f"  -> {len(sito_map)} siti mappati (MAG_SITO_COD -> codice canonico)")

        print(f"\nQuery CDT_DW.F_CARICO...")
        oracle_kpi = query_oracle_kpi(conn, args.da, args.a, siti_filter, args.per_mese, sito_map)
        print(f"  ->{len(oracle_kpi)} chiavi in CDT_DW")
    finally:
        conn.close()

    print("\nLettura Gold F_CARICO (parquet locale)...")
    gold_kpi = query_gold_kpi(args.da, args.a, siti_filter, args.per_mese)
    print(f"  -> {len(gold_kpi)} chiavi in Gold")

    exit_code = print_report(oracle_kpi, gold_kpi, args.soglia, args.per_mese)
    sys.exit(1 if exit_code > 0 else 0)


if __name__ == "__main__":
    main()
