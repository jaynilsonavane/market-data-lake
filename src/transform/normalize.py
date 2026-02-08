from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

def normalize_ohlcv(cfg: dict) -> Path:
    base = Path(cfg["storage"]["base_dir"])
    bronze_root = base / cfg["storage"]["bronze_dir"] / "source=stooq"
    out_path = base / cfg["silver_tables"]["ohlcv_daily_path"]
    out_path.parent.mkdir(parents=True, exist_ok=True)

    frames = []
    for raw_csv in bronze_root.rglob("raw.csv"):
        # infer symbol from path: .../symbol=XYZ/...
        symbol = [p for p in raw_csv.parts if p.startswith("symbol=")][0].split("=")[1]
        df = pd.read_csv(raw_csv)

        # Stooq columns typically: Date, Open, High, Low, Close, Volume
        df.rename(columns={
            "Date": "ts",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }, inplace=True)

        df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.date.astype("datetime64[ns]")
        df["symbol"] = symbol
        df["source"] = "stooq"
        df["ingested_at"] = datetime.now(timezone.utc)

        frames.append(df[["source","symbol","ts","open","high","low","close","volume","ingested_at"]])

    all_df = pd.concat(frames, ignore_index=True)

    # Deduplicate: keep latest ingested row
    all_df = all_df.sort_values(["source","symbol","ts","ingested_at"]).drop_duplicates(
        subset=["source","symbol","ts"],
        keep="last"
    )

    all_df.to_parquet(out_path, index=False)
    return out_path
