from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import financedatabase as fd

def build_universe(cfg: dict) -> Path:
    symbols = cfg["universe"]["symbols"]
    base = Path(cfg["storage"]["base_dir"])
    out_path = base / cfg["silver_tables"]["dim_assets_path"]
    out_path.parent.mkdir(parents=True, exist_ok=True)

    equities = fd.Equities()

    rows = []
    # FinanceDatabase uses an internal index of symbols; we'll try to match by common ticker.
    # If metadata isn't found, we still include the symbol (important: pipeline should not break).
    for sym in symbols:
        meta = None
        try:
            # select() returns a DataFrame; try a few common fields
            df = equities.select(symbol=sym)  # may return empty depending on availability
            if df is not None and len(df) > 0:
                meta = df.iloc[0].to_dict()
        except Exception:
            meta = None

        rows.append({
            "symbol": sym,
            "name": (meta or {}).get("name"),
            "exchange": (meta or {}).get("exchange"),
            "country": (meta or {}).get("country"),
            "sector": (meta or {}).get("sector"),
            "currency": (meta or {}).get("currency"),
            "asset_class": "equity",
            "created_at": datetime.now(timezone.utc).isoformat(),
        })

    dim_assets = pd.DataFrame(rows)
    dim_assets.to_parquet(out_path, index=False)
    return out_path
