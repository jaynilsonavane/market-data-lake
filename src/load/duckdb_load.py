from __future__ import annotations
from pathlib import Path
import duckdb

def load_duckdb(cfg: dict) -> None:
    base = Path(cfg["storage"]["base_dir"])
    dim_path = base / cfg["silver_tables"]["dim_assets_path"]
    ohlcv_path = base / cfg["silver_tables"]["ohlcv_daily_path"]
    db_path = Path(cfg["storage"]["duckdb_path"])
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    con.execute("CREATE OR REPLACE TABLE dim_assets AS SELECT * FROM read_parquet(?)", [str(dim_path)])
    con.execute("CREATE OR REPLACE TABLE ohlcv_daily AS SELECT * FROM read_parquet(?)", [str(ohlcv_path)])
    con.close()
