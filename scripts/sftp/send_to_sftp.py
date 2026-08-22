#!/usr/bin/env python3
"""
send_to_sftp.py — Invio dati landing → area SFTP Azure (release kit KIT-01).

Invia i file di landing (CSV/Parquet) all'endpoint SFTP di Azure, con struttura
remota ``<base>/<sistema>/YYYY/MM/DD/<file>``. Idempotente (salta i file già
presenti con stessa dimensione), con retry e logging.

⚠️ Parametri SFTP da integrare quando disponibili (host/porta/utente/credenziali):
si passano via **variabili d'ambiente** o CLI — MAI hardcoded, MAI su git.
    SFTP_HOST, SFTP_PORT (def 22), SFTP_USER,
    SFTP_PASSWORD  *oppure*  SFTP_KEY_PATH (chiave privata),
    SFTP_REMOTE_BASE (dir remota base)

Il **dry-run** (default: --dry-run) NON si connette: stampa il piano di upload.
È testabile subito, senza credenziali e senza paramiko installato.

Oracle resta READ-ONLY: questo script invia solo file già estratti nel landing.
Dati reali mai versionati: qui viaggia solo il codice.

Esempi:
    # anteprima (nessuna connessione):
    python send_to_sftp.py --landing /workspace/data/landing --run-date 2026-06-10 --dry-run
    # invio reale (richiede env SFTP_* + paramiko):
    SFTP_HOST=... SFTP_USER=... SFTP_PASSWORD=... SFTP_REMOTE_BASE=/data \
      python send_to_sftp.py --landing /workspace/data/landing --run-date 2026-06-10 --send
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

DEFAULT_FORMATS = (".csv", ".parquet")
RETRY_MAX = 3
RETRY_BACKOFF_S = 5


@dataclass
class SftpConfig:
    host: str
    user: str
    port: int = 22
    password: Optional[str] = None
    key_path: Optional[str] = None
    remote_base: str = "/"

    @classmethod
    def from_env(cls) -> Optional["SftpConfig"]:
        host = os.environ.get("SFTP_HOST")
        user = os.environ.get("SFTP_USER")
        if not host or not user:
            return None
        return cls(
            host=host, user=user,
            port=int(os.environ.get("SFTP_PORT", "22")),
            password=os.environ.get("SFTP_PASSWORD"),
            key_path=os.environ.get("SFTP_KEY_PATH"),
            remote_base=os.environ.get("SFTP_REMOTE_BASE", "/"),
        )


@dataclass
class UploadItem:
    local_path: Path
    remote_path: str
    size: int
    system: str = ""


def _log(msg: str) -> None:
    print(f"[send_to_sftp] {msg}", flush=True)


def build_upload_plan(landing: Path, run_date: str, remote_base: str,
                      systems: Optional[List[str]] = None,
                      formats: Tuple[str, ...] = DEFAULT_FORMATS,
                      layout: str = "mirror") -> List[UploadItem]:
    """Costruisce il piano di upload: file landing → path remoto.

    ``layout``:
      - ``mirror`` (default): ``<base>/<sistema>/<relpath>`` — preserva la struttura
        del landing, che **contiene già** ``<tabella>/YYYY/MM/DD/`` (evita la doppia data).
      - ``datefirst``: ``<base>/YYYY/MM/DD/<sistema>/<relpath>`` — impone la data di
        ingestion in testa (usare se l'SFTP target organizza per data di invio).

    ``<sistema>`` = sottocartella di primo livello del landing (es. logistix-landing).
    NB: la convenzione remota definitiva va confermata col setup SFTP (KIT-01 integrazione).
    """
    yyyy, mm, dd = run_date[:4], run_date[5:7], run_date[8:10]
    base = remote_base.rstrip("/")
    plan: List[UploadItem] = []
    if not landing.exists():
        _log(f"ATTENZIONE: landing non esistente: {landing}")
        return plan

    for system_dir in sorted(p for p in landing.iterdir() if p.is_dir()):
        system = system_dir.name
        if systems and system not in systems:
            continue
        for f in sorted(system_dir.rglob("*")):
            if not f.is_file() or f.suffix.lower() not in formats:
                continue
            rel = f.relative_to(system_dir).as_posix()
            if layout == "datefirst":
                remote = f"{base}/{yyyy}/{mm}/{dd}/{system}/{rel}"
            else:  # mirror
                remote = f"{base}/{system}/{rel}"
            plan.append(UploadItem(local_path=f, remote_path=remote,
                                   size=f.stat().st_size, system=system))
    return plan


def _sftp_connect(cfg: SftpConfig):
    """Apre una connessione SFTP (import lazy di paramiko)."""
    try:
        import paramiko  # noqa: PLC0415
    except ImportError as e:
        raise RuntimeError(
            "paramiko non installato: aggiungerlo ai requirements per l'invio reale "
            "(`pip install paramiko`). Il --dry-run non lo richiede."
        ) from e

    transport = paramiko.Transport((cfg.host, cfg.port))
    if cfg.key_path:
        pkey = paramiko.RSAKey.from_private_key_file(cfg.key_path)
        transport.connect(username=cfg.user, pkey=pkey)
    else:
        transport.connect(username=cfg.user, password=cfg.password)
    return transport, paramiko.SFTPClient.from_transport(transport)


def _ensure_remote_dirs(sftp, remote_path: str) -> None:
    """Crea ricorsivamente le directory remote del file (mkdir -p)."""
    from posixpath import dirname
    d = dirname(remote_path)
    parts = [p for p in d.split("/") if p]
    cur = "/" if d.startswith("/") else ""
    for p in parts:
        cur = f"{cur}{p}/" if cur.endswith("/") or cur == "" else f"{cur}/{p}"
        cur = cur if cur.startswith("/") else "/" + cur
        try:
            sftp.stat(cur)
        except IOError:
            sftp.mkdir(cur)


def _remote_size(sftp, remote_path: str) -> Optional[int]:
    try:
        return sftp.stat(remote_path).st_size
    except IOError:
        return None


def send(plan: List[UploadItem], cfg: SftpConfig) -> Tuple[int, int, int]:
    """Esegue l'upload. Idempotente (salta stessa dimensione) + retry.

    Ritorna (inviati, saltati, falliti).
    """
    transport, sftp = _sftp_connect(cfg)
    sent = skipped = failed = 0
    try:
        for it in plan:
            if _remote_size(sftp, it.remote_path) == it.size:
                skipped += 1
                continue
            ok = False
            for attempt in range(1, RETRY_MAX + 1):
                try:
                    _ensure_remote_dirs(sftp, it.remote_path)
                    sftp.put(str(it.local_path), it.remote_path)
                    ok = True
                    break
                except Exception as e:  # noqa: BLE001
                    _log(f"retry {attempt}/{RETRY_MAX} {it.remote_path}: {e}")
                    time.sleep(RETRY_BACKOFF_S * attempt)
            if ok:
                sent += 1
            else:
                failed += 1
                _log(f"FALLITO: {it.local_path} -> {it.remote_path}")
    finally:
        sftp.close()
        transport.close()
    return sent, skipped, failed


def main(argv: List[str]) -> int:
    from datetime import date
    p = argparse.ArgumentParser(description="Invio landing -> SFTP Azure (KIT-01)")
    p.add_argument("--landing", required=True, help="Directory landing locale")
    p.add_argument("--run-date", default=str(date.today()), help="YYYY-MM-DD (struttura remota)")
    p.add_argument("--remote-base", default=os.environ.get("SFTP_REMOTE_BASE", "/data"),
                   help="Dir remota base (o env SFTP_REMOTE_BASE)")
    p.add_argument("--systems", default="", help="Filtro sistemi (csv), vuoto=tutti")
    p.add_argument("--layout", choices=["mirror", "datefirst"], default="mirror",
                   help="mirror=preserva struttura landing (default); datefirst=YYYY/MM/DD in testa")
    p.add_argument("--dry-run", action="store_true", default=True, help="Solo piano, nessuna connessione (default)")
    p.add_argument("--send", dest="dry_run", action="store_false", help="Esegue l'invio reale (richiede SFTP_* + paramiko)")
    args = p.parse_args(argv)

    systems = [s.strip() for s in args.systems.split(",") if s.strip()] or None
    plan = build_upload_plan(Path(args.landing), args.run_date, args.remote_base, systems,
                             layout=args.layout)

    tot_mb = sum(it.size for it in plan) / (1024 * 1024)
    _log(f"piano: {len(plan)} file, {tot_mb:.1f} MB, run_date={args.run_date}, base={args.remote_base}")
    # riepilogo per sistema
    by_sys: dict = {}
    for it in plan:
        by_sys[it.system] = by_sys.get(it.system, 0) + 1
    for k, v in sorted(by_sys.items()):
        _log(f"  {k}: {v} file")

    if args.dry_run:
        _log("DRY-RUN: nessuna connessione eseguita. Esempi di destinazione:")
        for it in plan[:5]:
            _log(f"  {it.local_path.name}  ->  {it.remote_path}")
        _log("Per inviare davvero: --send con env SFTP_HOST/SFTP_USER/SFTP_PASSWORD|SFTP_KEY_PATH.")
        return 0

    cfg = SftpConfig.from_env()
    if cfg is None:
        _log("ERRORE: --send richiede almeno SFTP_HOST e SFTP_USER nell'ambiente.")
        return 2
    cfg.remote_base = args.remote_base
    sent, skipped, failed = send(plan, cfg)
    _log(f"RISULTATO: inviati={sent} saltati={skipped} falliti={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
