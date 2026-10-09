r"""
compare_f_carico.py — Confronto MISURE F_CARICO: CDT_DW (ODI, estratto row-level) vs gold CLOUD.

Fase 3 quadratura (ACT_9029, Q-01). Legge:
  - ODI:  parquet estratti da extract_f_carico.py (LOGISTICO_DATA/quadratura/f_carico/cdtdw/AAAA/MM/GG/)
  - Gold: gold_dev.logistica.f_carico via SQL warehouse (databricks-sdk, Statement Execution)

Grain di confronto = ETICHETTA: (SITO_COD canonico, NUM_DOC_CARICO, NUM_ETICH).
Misure (default): QTA_CARICO, QTA_UF_CARICO, QTA_ORD_FORN, PES_CARICO, VOL_CARICO, VAL_COSTO_CARICO.
NESSUNA soglia: evidenzia ogni differenza. Il report è bounded (sommari + campioni), non 25k righe.

NB copertura (OP-QDR-1): il gold DEV copre solo alcuni siti/giorni; le chiavi "solo ODI" sono
tipicamente siti non caricati, non errori di calcolo. Il confronto misure ha senso sulle chiavi COMUNI.

USO: py -3.12 compare_f_carico.py --da 2026-09-22 --a 2026-09-22 [--warehouse <id>]
"""
from __future__ import annotations
import argparse, os, re, sys, datetime as dt
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")   # console Windows cp1252 -> evita UnicodeEncodeError
except Exception:
    pass
try:
    import pandas as pd, pyarrow.parquet as pq
except ImportError:
    print("[ERROR] manca pandas/pyarrow", file=sys.stderr); sys.exit(2)
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState, Disposition, Format
except ImportError:
    print("[ERROR] manca databricks-sdk: py -3.12 -m pip install databricks-sdk", file=sys.stderr); sys.exit(2)

DATA_ROOT = Path(os.environ.get("LOGISTICO_DATA", r"C:\PROGETTI\LOGISTICO_DATA"))
ODI_ROOT  = DATA_ROOT / "quadratura" / "f_carico" / "cdtdw"
GOLD_TBL  = "gold_dev.logistica.f_carico"
DEFAULT_WH = "2a59dde49ff3e35d"  # Warehouse-Engineering-Serverless
MEASURES  = ["QTA_CARICO", "QTA_UF_CARICO", "QTA_ORD_FORN", "PES_CARICO", "VOL_CARICO", "VAL_COSTO_CARICO"]

def norm_sito(v) -> str | None:
    if v is None: return None
    d = re.sub(r"[^0-9]", "", str(v))
    return str(int(d)).zfill(2) if d else None

def norm_num(v) -> str | None:
    if v is None or (isinstance(v, float) and pd.isna(v)): return None
    try: return str(int(float(v)))          # 12345.0 -> "12345" (allinea ODI number a gold string)
    except (ValueError, TypeError): return str(v).strip()

def days(da, a):
    d0, d1 = dt.date.fromisoformat(da), dt.date.fromisoformat(a)
    while d0 <= d1:
        yield d0; d0 += dt.timedelta(days=1)

def load_odi(da, a) -> pd.DataFrame:
    frames = []
    for d in days(da, a):
        p = ODI_ROOT / f"{d.year:04d}" / f"{d.month:02d}" / f"{d.day:02d}" / "f_carico.parquet"
        if p.exists():
            frames.append(pq.read_table(p).to_pandas())
    if not frames:
        raise SystemExit(f"[ERROR] nessun parquet ODI in {ODI_ROOT} per {da}..{a} (estrai con extract_f_carico.py)")
    df = pd.concat(frames, ignore_index=True)
    df["SITO"] = df["MAG_SITO_COD"].map(norm_sito)
    df["NDOC"] = df["NUM_DOC_CARICO"].map(norm_num)
    df["NETI"] = df["NUM_ETICH"].map(norm_num)
    for m in MEASURES:
        df[m] = pd.to_numeric(df.get(m), errors="coerce").fillna(0.0)
    return df

def load_gold(da, a, wh) -> pd.DataFrame:
    w = WorkspaceClient()
    meas = ", ".join(f"SUM({m}) AS {m}" for m in MEASURES)
    sql = (f"SELECT CAST(SITO_COD AS STRING) SITO, CAST(NUM_DOC_CARICO AS STRING) NDOC, "
           f"CAST(NUM_ETICH AS STRING) NETI, COUNT(*) CNT, {meas} FROM {GOLD_TBL} "
           f"WHERE DATA_CARICO BETWEEN DATE'{da}' AND DATE'{a}' GROUP BY 1,2,3")
    r = w.statement_execution.execute_statement(warehouse_id=wh, statement=sql, wait_timeout="50s",
                                                disposition=Disposition.INLINE, format=Format.JSON_ARRAY)
    if r.status.state != StatementState.SUCCEEDED:
        raise SystemExit(f"[ERROR] gold query: {r.status.error}")
    rows = list(r.result.data_array or [])
    # segue i chunk successivi se presenti
    chunk = r.result.next_chunk_index
    while chunk is not None:
        c = w.statement_execution.get_statement_result_chunk_n(r.statement_id, chunk)
        rows += list(c.data_array or []); chunk = c.next_chunk_index
    cols = ["SITO", "NDOC", "NETI", "CNT"] + MEASURES
    df = pd.DataFrame(rows, columns=cols)
    df["SITO"] = df["SITO"].map(norm_sito)
    df["NDOC"] = df["NDOC"].map(norm_num); df["NETI"] = df["NETI"].map(norm_num)
    for m in MEASURES: df[m] = pd.to_numeric(df[m], errors="coerce").fillna(0.0)
    df["CNT"] = pd.to_numeric(df["CNT"], errors="coerce").fillna(0).astype(int)
    return df

def agg_key(df):
    g = df.groupby(["SITO", "NDOC", "NETI"], dropna=False)
    out = g[MEASURES].sum(); out["CNT"] = g.size(); return out.reset_index()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--da", required=True); ap.add_argument("--a", required=True)
    ap.add_argument("--warehouse", default=DEFAULT_WH)
    ap.add_argument("--sample", type=int, default=15, help="n. righe di esempio per i mismatch")
    args = ap.parse_args()

    print(f"ODI (parquet) {args.da}..{args.a} ...")
    odi = agg_key(load_odi(args.da, args.a))
    print(f"  ODI: {len(odi):,} chiavi, {int(odi['CNT'].sum()):,} righe")
    print(f"Gold cloud ({GOLD_TBL}) via warehouse {args.warehouse} ...")
    gold = agg_key(load_gold(args.da, args.a, args.warehouse))
    print(f"  Gold: {len(gold):,} chiavi, {int(gold['CNT'].sum()):,} righe")

    m = odi.merge(gold, on=["SITO", "NDOC", "NETI"], how="outer", suffixes=("_odi", "_gold"), indicator=True)
    matched = m[m["_merge"] == "both"]
    only_odi = m[m["_merge"] == "left_only"]; only_gold = m[m["_merge"] == "right_only"]

    print("\n=== COPERTURA (chiavi etichetta) ===")
    print(f"  comuni: {len(matched):,} | solo ODI: {len(only_odi):,} | solo Gold: {len(only_gold):,}")
    print("\n  Per sito (ODI vs Gold, n. chiavi):")
    cov = m.groupby("SITO").agg(odi=("CNT_odi", lambda s: s.notna().sum()),
                                gold=("CNT_gold", lambda s: s.notna().sum())).sort_index()
    for sito, row in cov.iterrows():
        print(f"    sito {sito}: ODI {int(row['odi']):>7,}  Gold {int(row['gold']):>7,}")

    print("\n=== MISURE sulle chiavi COMUNI (nessuna soglia) ===")
    if matched.empty:
        print("  (nessuna chiave comune)")
    else:
        for meas in MEASURES:
            co, cg = f"{meas}_odi", f"{meas}_gold"
            diff = (matched[co].fillna(0) - matched[cg].fillna(0)).abs()
            nmis = int((diff > 1e-9).sum())
            print(f"  {meas:<16} chiavi diverse: {nmis:>6,} | sum|delta|: {diff.sum():,.3f} | sumODI {matched[co].sum():,.2f} sumGold {matched[cg].sum():,.2f}")
        # campione mismatch
        anymis = matched[ sum((matched[f'{x}_odi'].fillna(0)-matched[f'{x}_gold'].fillna(0)).abs() for x in MEASURES) > 1e-9 ]
        if not anymis.empty:
            print(f"\n  Esempi mismatch ({min(args.sample,len(anymis))} di {len(anymis):,}):")
            for _, r in anymis.head(args.sample).iterrows():
                deltas = ", ".join(f"{x}:{r[f'{x}_odi']:.1f}/{r[f'{x}_gold']:.1f}" for x in MEASURES
                                   if abs((r[f'{x}_odi'] or 0)-(r[f'{x}_gold'] or 0))>1e-9)
                print(f"    sito {r['SITO']} doc {r['NDOC']} eti {r['NETI']} | {deltas}")
    print()

if __name__ == "__main__":
    main()
