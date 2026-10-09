r"""
extract_f_carico.py — Estrae CDT_DW.F_CARICO (ROW-LEVEL) per giorni, per la quadratura vs Gold.

READ-ONLY assoluto: solo SELECT su CDT_DW. Riusa connessione/mapping di quadratura_f_carico.py
(stesso .env in scripts/landing_simulator/). Complementare ai tool di quadratura esistenti
(quadratura_fact.py / quadratura_f_carico.py) che confrontano solo KPI aggregati: qui si portano
giu' i DATI grezzi, giorno per giorno, per il confronto strutturale (fase 1) e di dettaglio (fase 2).

USO (ACT_9029, Q-01):
    # Fase 1 — struttura: colonne + numero + chiavi di CDT_DW.F_CARICO
    py -3 extract_f_carico.py --discover

    # Fase 1 — struttura lato Gold (parquet locale): colonne + numero
    py -3 extract_f_carico.py --gold-schema

    # Estrazione dati per intervallo di giorni -> parquet per-giorno
    py -3 extract_f_carico.py --da 2026-06-09 --a 2026-06-23
    py -3 extract_f_carico.py --da 2026-06-09 --a 2026-06-23 --siti LAIX,LBVX --out C:\PROGETTI\LOGISTICO_DATA\quadratura

OUTPUT (default):
    <LOGISTICO_DATA>/quadratura/f_carico/cdtdw/AAAA/MM/GG/f_carico.parquet   (una partizione per giorno)

DIPENDENZE: oracledb, python-dotenv, pandas, pyarrow  (come gli altri tool di quadratura).
CONNESSIONE: .env in scripts/landing_simulator/ (ORACLE_HOST/PORT/SERVICE/USER/PASSWORD) + VPN attiva.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import sys
from pathlib import Path

# Riuso della connessione/mapping gia' validati (stesso dir)
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import quadratura_f_carico as qfc  # noqa: E402  (connect_oracle, build_sito_map, _normalize_sito_code, GOLD_PATH)

try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("[ERROR] manca pandas/pyarrow. Installa: pip install pandas pyarrow", file=sys.stderr)
    sys.exit(2)

ORACLE_SCHEMA = "CDT_DW"
ORACLE_TABLE = "F_CARICO"
GIORNO_FK = "GIORNO_CARICO_ID"   # FK -> CDT_DW.L_GIORNO.GIORNO_ID (semantica data validata dai tool quadratura)

DATA_ROOT = Path(os.environ.get("LOGISTICO_DATA", r"C:\PROGETTI\LOGISTICO_DATA"))
DEFAULT_OUT = DATA_ROOT / "quadratura"


# ────────────────────────────────────────────────────────────────────────────
# FASE 1 — struttura
# ────────────────────────────────────────────────────────────────────────────
def discover(conn) -> None:
    """Colonne + tipi + numero + chiavi (PK/unique) di CDT_DW.F_CARICO."""
    col_sql = """
        SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, NULLABLE
        FROM ALL_TAB_COLUMNS WHERE OWNER=:o AND TABLE_NAME=:t ORDER BY COLUMN_ID
    """
    key_sql = """
        SELECT c.CONSTRAINT_TYPE, cc.COLUMN_NAME, cc.POSITION, c.CONSTRAINT_NAME
        FROM ALL_CONSTRAINTS c
        JOIN ALL_CONS_COLUMNS cc ON cc.OWNER=c.OWNER AND cc.CONSTRAINT_NAME=c.CONSTRAINT_NAME
        WHERE c.OWNER=:o AND c.TABLE_NAME=:t AND c.CONSTRAINT_TYPE IN ('P','U')
        ORDER BY c.CONSTRAINT_TYPE, c.CONSTRAINT_NAME, cc.POSITION
    """
    with conn.cursor() as cur:
        cur.execute(col_sql, o=ORACLE_SCHEMA, t=ORACLE_TABLE)
        cols = cur.fetchall()
        cur.execute(key_sql, o=ORACLE_SCHEMA, t=ORACLE_TABLE)
        keys = cur.fetchall()

    print(f"\n== {ORACLE_SCHEMA}.{ORACLE_TABLE} — colonne ({len(cols)}) ==")
    print(f"  {'#':>3}  {'COLUMN_NAME':<32} {'TYPE':<18} NULL")
    print("  " + "-" * 62)
    for i, (name, dtype, length, nullable) in enumerate(cols, 1):
        print(f"  {i:>3}  {name:<32} {dtype:<18} {nullable}")
    print(f"\n  NUMERO COLONNE: {len(cols)}")

    print(f"\n== Chiavi dichiarate (PK/Unique) ==")
    if not keys:
        print("  Nessuna PK/Unique dichiarata sulla tabella (tipico dei fact ODI).")
        print("  Grain noto da certifica (ACT_9001): ETICHETTA = SITO + NUM_DOC + NUM_ETICH")
        print("  -> usare --discover per individuare le colonne reali corrispondenti.")
    else:
        cur_name = None
        for ctype, col, pos, cname in keys:
            if cname != cur_name:
                kind = "PK" if ctype == "P" else "UNIQUE"
                print(f"  [{kind}] {cname}:")
                cur_name = cname
            print(f"      - {col}")


def gold_schema() -> None:
    """Colonne + numero del Gold F_CARICO (parquet locale)."""
    gp: Path = qfc.GOLD_PATH
    if not gp.exists():
        print(f"[!] Gold F_CARICO locale non trovato: {gp}")
        print("    (se il gold e' solo su cloud Databricks, la fase 2 andra' confrontata li'.)")
        return
    # trova un file parquet qualsiasi (la tabella e' partizionata per ANNO_MESE)
    sample = next(gp.rglob("*.parquet"), None)
    if sample is None:
        print(f"[!] Nessun parquet sotto {gp}")
        return
    schema = pq.read_schema(sample)
    names = [f.name for f in schema]
    print(f"\n== Gold F_CARICO (parquet locale) — colonne ({len(names)}) ==")
    print(f"  fonte: {sample}")
    for i, n in enumerate(names, 1):
        print(f"  {i:>3}  {n}")
    print(f"\n  NUMERO COLONNE (Gold, +partizione ANNO_MESE): {len(names)}")


# ────────────────────────────────────────────────────────────────────────────
# Estrazione row-level per giorno
# ────────────────────────────────────────────────────────────────────────────
def _days(da: str, a: str):
    d0 = dt.date.fromisoformat(da)
    d1 = dt.date.fromisoformat(a)
    if d1 < d0:
        raise SystemExit("[ERROR] --a precedente a --da")
    d = d0
    while d <= d1:
        yield d
        d += dt.timedelta(days=1)


def extract(conn, da: str, a: str, siti_filter, out_root: Path) -> None:
    # filtro siti (canonico -> MAG_SITO_COD grezzi), come quadratura_f_carico
    sito_clause, sito_binds = "", {}
    if siti_filter:
        sito_map = qfc.build_sito_map(conn)
        want = {s.strip().upper() for s in siti_filter}
        mag = [m for m, norm in sito_map.items() if norm in want]
        if mag:
            ph = ", ".join(f":s{i}" for i in range(len(mag)))
            sito_clause = f"AND TRIM(f.MAG_SITO_COD) IN ({ph})"
            sito_binds = {f"s{i}": s for i, s in enumerate(mag)}
        else:
            print(f"[!] nessun MAG_SITO_COD per i siti {sorted(want)} -> estrazione vuota")

    base_out = out_root / "f_carico" / "cdtdw"
    tot_rows = 0
    for day in _days(da, a):
        ds = day.isoformat()
        sql = f"""
            SELECT f.*, g.GIORNO_DT
            FROM {ORACLE_SCHEMA}.{ORACLE_TABLE} f
            JOIN {ORACLE_SCHEMA}.L_GIORNO g ON g.GIORNO_ID = f.{GIORNO_FK}
            WHERE g.GIORNO_DT = TO_DATE(:d,'YYYY-MM-DD')
            {sito_clause}
        """
        binds = {"d": ds, **sito_binds}
        with conn.cursor() as cur:
            cur.arraysize = 5000
            cur.execute(sql, binds)
            colnames = [c[0] for c in cur.description]
            rows = cur.fetchall()
        if not rows:
            print(f"  {ds}: 0 righe (skip)")
            continue
        df = pd.DataFrame(rows, columns=colnames)
        dst_dir = base_out / f"{day.year:04d}" / f"{day.month:02d}" / f"{day.day:02d}"
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / "f_carico.parquet"
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), dst)
        tot_rows += len(df)
        print(f"  {ds}: {len(df):,d} righe -> {dst}")
    print(f"\nTOTALE: {tot_rows:,d} righe estratte in {base_out}")


def main() -> None:
    p = argparse.ArgumentParser(description="Estrae CDT_DW.F_CARICO row-level per giorni (quadratura Q-01).")
    p.add_argument("--discover", action="store_true", help="Mostra colonne+tipi+numero+chiavi di CDT_DW.F_CARICO ed esce")
    p.add_argument("--gold-schema", action="store_true", help="Mostra colonne+numero del Gold F_CARICO (parquet locale) ed esce")
    p.add_argument("--da", metavar="YYYY-MM-DD", help="Data inizio (inclusa)")
    p.add_argument("--a", metavar="YYYY-MM-DD", help="Data fine (inclusa)")
    p.add_argument("--siti", default="", help="Filtro siti canonici (es. LAIX,LBVX); default tutti")
    p.add_argument("--out", default=str(DEFAULT_OUT), help=f"Dir output (default {DEFAULT_OUT})")
    args = p.parse_args()

    if args.gold_schema:
        gold_schema()
        return

    for x in (args.da, args.a):
        if x and not re.match(r"^\d{4}-\d{2}-\d{2}$", x):
            p.error("le date devono essere YYYY-MM-DD")

    if not args.discover and not (args.da and args.a):
        p.error("serve --discover, --gold-schema, oppure --da e --a per l'estrazione")

    print("Connessione Oracle (CDT_DW)...")
    conn = qfc.connect_oracle()
    try:
        if args.discover:
            discover(conn)
            return
        siti = [s.strip().upper() for s in args.siti.split(",") if s.strip()] or None
        print(f"Estrazione {ORACLE_SCHEMA}.{ORACLE_TABLE}: {args.da} -> {args.a} | siti: {siti or 'tutti'}")
        extract(conn, args.da, args.a, siti, Path(args.out))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
