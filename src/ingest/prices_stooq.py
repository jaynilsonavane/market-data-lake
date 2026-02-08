from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import requests

def _stooq_url(symbol: str) -> str:
    # Stooq uses lower-case tickers for US equities, often with .us suffix.
    # We'll use {symbol}.us by default; you can override later with source_symbol mapping.
    return f"https://stooq.com/q/d/l/?s={symbol.lower()}.us&i=d"

def ingest_prices(cfg: dict) -> list[Path]:
    base = Path(cfg["storage"]["base_dir"])
    bronze_root = base / cfg["storage"]["bronze_dir"] / "source=stooq"
    bronze_root.mkdir(parents=True, exist_ok=True)

    dim_path = base / cfg["silver_tables"]["dim_assets_path"]
    dim = pd.read_parquet(dim_path)

    out_files: list[Path] = []
    run_date = datetime.now(timezone.utc).date().isoformat()

    for sym in dim["symbol"].tolist():
        url = _stooq_url(sym)
        r = requests.get(url, timeout=30)
        r.raise_for_status()

        sym_dir = bronze_root / f"symbol={sym}" / f"date={run_date}"
        sym_dir.mkdir(parents=True, exist_ok=True)
        out_path = sym_dir / "raw.csv"
        out_path.write_bytes(r.content)
        out_files.append(out_path)

    return out_files
