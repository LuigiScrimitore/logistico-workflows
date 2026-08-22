"""
Estrattore lookup anagrafiche CDT_DW -> landing cdtdw-landing/ (READ-ONLY).

Workaround temporaneo in attesa di OP-02 (Retail Master Data da Reply):
porta le anagrafiche master gia' presenti nel DWH legacy CDT_DW in uno schema
isolato 'cdtdw', cosi' da poter agganciare i fact Gold SUBITO. Quando arrivera'
il flusso Retail definitivo, basta ripuntare retail_master_schema.

Doppio scopo futuro: lo stesso schema sara' utile per le quadrature
legacy <-> nuovo (fase separata, vedi backlog).

IMPORTANTE: esegue ESCLUSIVAMENTE SELECT. Nessuna scrittura/DDL su CDT_DW.
Riusa connessione e writer dello strumento di landing esistente per non
duplicare la logica Oracle.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

# Riuso del modulo di landing esistente (connessione + writer + logging)
HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent.parent
LANDING_SIM = REPO_ROOT / "scripts" / "landing_simulator"
sys.path.insert(0, str(LANDING_SIM))

import extract_oracle_to_landing as base  # noqa: E402

logger = logging.getLogger("cdtdw_lookups")

# ─── Tabelle lookup CDT_DW da estrarre (full snapshot) ──────────────────────
# Mapping: tabella legacy CDT_DW -> nome target LU_* (allineato Excel/OP-02).
# Il rename L_* -> LU_* avviene nel notebook builder (gold layer), qui si
# estrae il dato grezzo con il nome legacy.
SCHEMA = "CDT_DW"
# Valore = stringa (target, schema=CDT_DW, SELECT *) oppure dict con chiavi:
#   target : nome target LU_*
#   schema : schema Oracle sorgente (default CDT_DW)
#   sql    : SQL SELECT completo custom (override; per proiezioni/filtri/join)
LOOKUP_TABLES = {
    "L_ART_RADICE": "LU_ART_RADICE",   # articolo radice (693k)
    "L_FORN":       "LU_FORNITORE",     # fornitore (11k)
    "L_PDV":        "LU_PDV",           # punto vendita (4k)
    "L_GIORNO":     "LU_GIORNO",        # calendario giornaliero (8k)
    "L_MESE":       "LU_MESE",          # calendario mensile (264)
    # Anagrafica unità logistica articolo: peso lordo + dimensioni pezzo, base del
    # calcolo ODI di PES_CARICO/VOL_CARICO (CDT_SA.SP_LOAD_F_CARICO). Riproduce la
    # subquery "UL": LU_ART_UNITA_LOGISTICA (COD=1) ⋈ LU_ART_RADICE per ART_MODL_PES_COD.
    "LU_ART_UNITA_LOGISTICA": {
        "target": "LU_ART_UNITA_LOGISTICA",
        "schema": "CDT_DWH_EDW",
        # Chiave articolo esposta = ART_RADICE + variante LOGISTICA (ART_VARIANTE_LOGISTICA_ID),
        # stesso asse del grain interno (join pesata↔dettaglio). Niente variante vendita.
        # AUDIT_ID in proiezione = watermark incrementale (max landato -> --audit-watermark).
        "sql": (
            "SELECT B.ART_RADICE_COD, B.ART_VARIANTE_LOGIS_COD, "
            "B.ART_VARIANTE_LOGISTICA_ID, A.ART_MODL_PES_COD, "
            "B.ART_UNITA_LOGISTICA_PESO_LORDO, B.ART_UNITA_LOGISTICA_ALT_PZ, "
            "B.ART_UNITA_LOGISTICA_LAR_PZ, B.ART_UNITA_LOGISTICA_PRO_PZ, B.AUDIT_ID "
            "FROM CDT_DWH_EDW.LU_ART_UNITA_LOGISTICA B "
            "LEFT JOIN CDT_DWH_EDW.LU_ART_RADICE A ON A.ART_RADICE_COD = B.ART_RADICE_COD "
            "WHERE B.ART_UNITA_LOGISTICA_COD = 1"
        ),
        # Colonna watermark per l'estrazione incrementale (delta). Se --audit-watermark > 0
        # viene appeso "AND <audit_col> > <wm>". Baseline attuale (max, 2026-07-02): 17832875.
        "audit_col": "B.AUDIT_ID",
    },
}

CSV_CFG = {"separator": ";", "encoding": "utf-8", "null_value": ""}


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description="Estrai lookup CDT_DW (read-only) -> cdtdw-landing")
    p.add_argument("--output-dir", default=r"C:/PROGETTI/LOGISTICO_DATA/data/landing",
                   help="Root cartella landing (fuori dal repo)")
    p.add_argument("--run-date", default=str(date.today()), help="YYYY-MM-DD")
    p.add_argument("--max-rows", type=int, default=0,
                   help="Cap righe per tabella (0 = nessun cap). Safety net per L_ART_RADICE.")
    p.add_argument("--tables", default=None, help="Filtro tabelle (es. L_FORN,L_PDV)")
    p.add_argument("--audit-watermark", type=int, default=0,
                   help="Estrazione incrementale: per le tabelle con 'audit_col' estrae solo "
                        "le righe con AUDIT_ID > watermark (0 = full/baseline).")
    p.add_argument("--env-file", default=str(LANDING_SIM / ".env"))
    args = p.parse_args(argv)

    # Carica .env (stesse credenziali CDT_ESTR; l'utente CDT_ESTR vede CDT_DW)
    if base.load_dotenv:
        envp = Path(args.env_file)
        if envp.exists():
            base.load_dotenv(envp)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")

    run_dt = date.fromisoformat(args.run_date)
    y, m, d = f"{run_dt:%Y}", f"{run_dt:%m}", f"{run_dt:%d}"
    out_root = Path(args.output_dir) / "cdtdw-landing"

    selected = None
    if args.tables:
        selected = {t.strip().upper() for t in args.tables.split(",") if t.strip()}

    conn = base.connect_oracle()
    logger.info("Connessione Oracle stabilita (READ-ONLY su %s).", SCHEMA)
    total_files = total_rows = errors = 0
    try:
        for tab, spec in LOOKUP_TABLES.items():
            if selected and tab not in selected:
                continue
            # Normalizza spec: stringa -> dict con default (schema CDT_DW, SELECT *)
            if isinstance(spec, str):
                spec = {"target": spec}
            target = spec["target"]
            if spec.get("sql"):
                sql = spec["sql"]   # SQL custom (già completo di schema/filtri)
            else:
                src = base._validate_ident(tab, "tabella")
                schema = base._validate_ident(spec.get("schema", SCHEMA), "schema")
                sql = f"SELECT * FROM {schema}.{src}"
            # Estrazione incrementale (delta): appende il filtro watermark se richiesto.
            audit_col = spec.get("audit_col")
            if audit_col and args.audit_watermark > 0:
                sql = f"{sql} AND {audit_col} > {int(args.audit_watermark)}"
                logger.info("  %s: modo DELTA (%s > %d)", tab, audit_col, args.audit_watermark)
            sql = base.apply_max_rows(sql, args.max_rows)
            out_file = out_root / tab / y / m / d / f"{tab}.csv"
            try:
                n = base.write_csv(conn, sql, {}, out_file, CSV_CFG,
                                   extra_col_mag_sito=None, merge_keys=[])
                total_files += 1
                total_rows += n
                logger.info("  %s -> %s : %d righe in %s", tab, target, n, out_file)
            except Exception as e:  # noqa: BLE001
                errors += 1
                logger.error("  ERRORE %s: %s", tab, str(e)[:160])
    finally:
        conn.close()
        logger.info("Connessione chiusa.")

    logger.info("Riepilogo: files=%d, rows=%d, errors=%d", total_files, total_rows, errors)
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
