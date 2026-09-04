#!/usr/bin/env python3
"""
send_to_landing.py — Invio dati landing → area di landing Azure (release kit KIT-01).

Trasporto **pluggable**: ``--transport azcopy`` (default, ADR-0023) | ``sftp`` (legacy).
Riusa la costruzione del piano di upload (`build_upload_plan`) e il backend SFTP da
`send_to_sftp.py`: qui cambia solo l'ULTIMO salto ("come si scrive"), non il "cosa".

AzCopy (default): copia i file su un container ADLS via ``azcopy copy``. A regime il
trasporto sarà orchestrato da processi ODI che invocano AzCopy; questo script resta un
**ponte/strumento di test** e un riferimento dei comandi/parametri AzCopy.

Auth (via **variabili d'ambiente**, MAI hardcoded, MAI su git):
  - **SAS**:  AZCOPY_DEST_URL = https://<account>.blob.core.windows.net/<container>
              AZCOPY_SAS      = '?sv=...&sig=...'   (token SAS con permessi di scrittura)
  - **AAD**:  AZCOPY_DEST_URL = ... (senza SAS) + login AAD:
              AZCOPY_AUTO_LOGIN_TYPE = MSI                         (Managed Identity)
              AZCOPY_AUTO_LOGIN_TYPE = SPN  + AZCOPY_SPA_APPLICATION_ID + AZCOPY_SPA_CLIENT_SECRET

Idempotenza: flag nativo ``--overwrite=ifSourceNewer`` (ricopia solo se la sorgente è più
recente). Il **dry-run** (default) NON esegue nulla e NON richiede azcopy installato:
stampa il piano e i comandi ``azcopy`` che verrebbero lanciati (SAS **mascherato**).

Oracle resta READ-ONLY: questo script invia solo file già estratti nel landing.
Dati reali mai versionati: qui viaggia solo il codice.

Esempi:
    # anteprima AzCopy (nessuna esecuzione, nessun tool/credenziale richiesti):
    python send_to_landing.py --landing ./landing --run-date 2026-06-10

    # invio reale via SAS:
    AZCOPY_DEST_URL=https://stdevdataplatformweudata.blob.core.windows.net/logisticolanding \
    AZCOPY_SAS='?sv=...&sig=...' \
      python send_to_landing.py --landing ./landing --run-date 2026-06-10 --send

    # trasporto legacy SFTP (invariato):
    python send_to_landing.py --landing ./landing --transport sftp
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List, Optional, Tuple

# Riuso della logica esistente (stesso folder): piano di upload + backend SFTP.
# Quando lo script gira, la sua directory è in sys.path[0] → import diretto.
from send_to_sftp import (  # noqa: E402
    DEFAULT_FORMATS,
    SftpConfig,
    UploadItem,
    build_upload_plan,
)
from send_to_sftp import send as sftp_send  # noqa: E402

AZCOPY_OVERWRITE_DEFAULT = "ifSourceNewer"


def _log(msg: str) -> None:
    print(f"[send_to_landing] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Backend AzCopy
# ---------------------------------------------------------------------------
@dataclass
class AzCopyConfig:
    dest_url: Optional[str] = None       # https://<account>.blob.core.windows.net/<container>
    sas: Optional[str] = None            # '?sv=...&sig=...'
    overwrite: str = AZCOPY_OVERWRITE_DEFAULT
    auto_login_type: Optional[str] = None  # MSI | SPN (usato se manca il SAS)

    @classmethod
    def from_env(cls) -> "AzCopyConfig":
        sas = os.environ.get("AZCOPY_SAS")
        if sas and not sas.startswith("?"):
            sas = "?" + sas
        return cls(
            dest_url=os.environ.get("AZCOPY_DEST_URL"),
            sas=sas,
            overwrite=os.environ.get("AZCOPY_OVERWRITE", AZCOPY_OVERWRITE_DEFAULT),
            auto_login_type=os.environ.get("AZCOPY_AUTO_LOGIN_TYPE"),
        )

    def auth_mode(self) -> str:
        if self.sas:
            return "SAS"
        if self.auto_login_type:
            return f"AAD/{self.auto_login_type.upper()}"
        return "NON CONFIGURATA"


_SAS_RE = re.compile(r"\?.*$")


def _mask_sas(url: str) -> str:
    """Nasconde la query string (SAS) nei log."""
    return _SAS_RE.sub("?<SAS>", url)


def azcopy_dest_url(cfg: AzCopyConfig, remote_path: str) -> str:
    """URL blob di destinazione per un file (con SAS se presente)."""
    base = (cfg.dest_url or "<AZCOPY_DEST_URL>").rstrip("/")
    key = remote_path.lstrip("/")
    url = f"{base}/{key}"
    if cfg.sas:
        url = f"{url}{cfg.sas}"
    return url


def azcopy_command(cfg: AzCopyConfig, item: UploadItem) -> List[str]:
    """Comando `azcopy copy` per un singolo file."""
    return [
        "azcopy", "copy",
        str(item.local_path),
        azcopy_dest_url(cfg, item.remote_path),
        f"--overwrite={cfg.overwrite}",
    ]


def _azcopy_available() -> bool:
    from shutil import which
    return which("azcopy") is not None


def azcopy_send(plan: List[UploadItem], cfg: AzCopyConfig) -> Tuple[int, int, int]:
    """Esegue l'upload via AzCopy (un `copy` per file). Ritorna (inviati, saltati, falliti).

    L'idempotenza è delegata ad AzCopy (`--overwrite=ifSourceNewer`): i file non più recenti
    non vengono ricopiati. AzCopy non espone un conteggio 'saltati' affidabile per-file, quindi
    'saltati' resta 0 e i no-op contano come inviati riusciti.
    """
    if cfg.dest_url is None:
        _log("ERRORE: --send azcopy richiede AZCOPY_DEST_URL nell'ambiente.")
        return 0, 0, len(plan)
    if not _azcopy_available():
        _log("ERRORE: 'azcopy' non trovato nel PATH. Installare AzCopy per l'invio reale.")
        return 0, 0, len(plan)
    if cfg.auth_mode() == "NON CONFIGURATA":
        _log("ERRORE: nessuna auth configurata (AZCOPY_SAS oppure AZCOPY_AUTO_LOGIN_TYPE).")
        return 0, 0, len(plan)

    sent = failed = 0
    for it in plan:
        cmd = azcopy_command(cfg, it)
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, check=False)  # noqa: S603
            if res.returncode == 0:
                sent += 1
            else:
                failed += 1
                _log(f"FALLITO ({res.returncode}): {it.remote_path} - {res.stderr.strip()[:200]}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            _log(f"FALLITO (eccezione): {it.remote_path} - {e}")
    return sent, 0, failed


def azcopy_dry_run(plan: List[UploadItem], cfg: AzCopyConfig, sample: int = 5) -> None:
    """Stampa i comandi azcopy che verrebbero eseguiti (SAS mascherato)."""
    _log(f"transport=azcopy  auth={cfg.auth_mode()}  overwrite={cfg.overwrite}")
    if cfg.dest_url is None:
        _log("NB: AZCOPY_DEST_URL non impostata -> mostro un placeholder <AZCOPY_DEST_URL>.")
    _log(f"comandi di esempio (primi {min(sample, len(plan))} di {len(plan)}):")
    for it in plan[:sample]:
        cmd = azcopy_command(cfg, it)
        shown = " ".join(_mask_sas(a) if a.startswith("http") else a for a in cmd)
        _log(f"  $ {shown}")
    _log("Per inviare davvero: --send con AZCOPY_DEST_URL + (AZCOPY_SAS | AZCOPY_AUTO_LOGIN_TYPE).")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _default_remote_base(transport: str) -> str:
    if transport == "sftp":
        return os.environ.get("SFTP_REMOTE_BASE", "/data")
    # azcopy: il container è la base → path relativo (nessun prefisso)
    return ""


def main(argv: List[str]) -> int:
    p = argparse.ArgumentParser(description="Invio landing → Azure (AzCopy/SFTP) — KIT-01 / ADR-0023")
    p.add_argument("--landing", required=True, help="Directory landing locale")
    p.add_argument("--run-date", default=str(date.today()), help="YYYY-MM-DD (struttura remota)")
    p.add_argument("--transport", choices=["azcopy", "sftp"], default="azcopy",
                   help="Backend di trasporto (default: azcopy, ADR-0023)")
    p.add_argument("--remote-base", default=None,
                   help="Base remota; default: '' per azcopy (container), $SFTP_REMOTE_BASE|/data per sftp")
    p.add_argument("--systems", default="", help="Filtro sistemi (csv), vuoto=tutti")
    p.add_argument("--layout", choices=["mirror", "datefirst"], default="mirror",
                   help="mirror=preserva struttura landing (default); datefirst=YYYY/MM/DD in testa")
    p.add_argument("--dry-run", action="store_true", default=True,
                   help="Solo piano/comandi, nessuna esecuzione (default)")
    p.add_argument("--send", dest="dry_run", action="store_false",
                   help="Esegue l'invio reale")
    args = p.parse_args(argv)

    remote_base = args.remote_base if args.remote_base is not None else _default_remote_base(args.transport)
    systems = [s.strip() for s in args.systems.split(",") if s.strip()] or None
    plan = build_upload_plan(Path(args.landing), args.run_date, remote_base, systems,
                             formats=DEFAULT_FORMATS, layout=args.layout)

    tot_mb = sum(it.size for it in plan) / (1024 * 1024)
    _log(f"piano: {len(plan)} file, {tot_mb:.1f} MB, run_date={args.run_date}, "
         f"transport={args.transport}, base={remote_base!r}")
    by_sys: dict = {}
    for it in plan:
        by_sys[it.system] = by_sys.get(it.system, 0) + 1
    for k, v in sorted(by_sys.items()):
        _log(f"  {k}: {v} file")

    if not plan:
        _log("Nessun file da inviare.")
        return 0

    # ---- AzCopy ----
    if args.transport == "azcopy":
        cfg = AzCopyConfig.from_env()
        if args.dry_run:
            azcopy_dry_run(plan, cfg)
            return 0
        sent, skipped, failed = azcopy_send(plan, cfg)
        _log(f"RISULTATO (azcopy): inviati={sent} saltati={skipped} falliti={failed}")
        return 0 if failed == 0 else 1

    # ---- SFTP (legacy, riuso backend esistente) ----
    if args.dry_run:
        _log("DRY-RUN (sftp): nessuna connessione. Esempi di destinazione:")
        for it in plan[:5]:
            _log(f"  {it.local_path.name}  ->  {it.remote_path}")
        _log("Per inviare davvero: --send con env SFTP_HOST/SFTP_USER/SFTP_PASSWORD|SFTP_KEY_PATH.")
        return 0
    cfg_sftp = SftpConfig.from_env()
    if cfg_sftp is None:
        _log("ERRORE: --send sftp richiede almeno SFTP_HOST e SFTP_USER nell'ambiente.")
        return 2
    cfg_sftp.remote_base = remote_base
    sent, skipped, failed = sftp_send(plan, cfg_sftp)
    _log(f"RISULTATO (sftp): inviati={sent} saltati={skipped} falliti={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
