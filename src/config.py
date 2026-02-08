from __future__ import annotations
from pathlib import Path
import yaml

def load_cfg(path: str = "config/config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # convenience: absolute base dir
    cfg["_base_dir"] = Path(cfg["storage"]["base_dir"])
    return cfg
