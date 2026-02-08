"""Market Data Lake — pipeline orchestrator.

Usage:
    python main.py                  # run full pipeline
    python main.py --step ingest    # run a single step
    python main.py --step transform
    python main.py --step load
"""
from __future__ import annotations

import argparse
import sys
import time

from src.config import load_cfg
from src.ingest.universe import build_universe
from src.ingest.prices_stooq import ingest_prices
from src.transform.normalize import normalize_ohlcv
from src.load.duckdb_load import load_duckdb


STEPS = {
    "ingest": ["universe", "prices"],
    "transform": ["normalize"],
    "load": ["duckdb"],
}


def run_step(name: str, func, *args) -> None:
    print(f"  ▶ {name} ...", end=" ", flush=True)
    t0 = time.perf_counter()
    result = func(*args)
    elapsed = time.perf_counter() - t0
    print(f"done ({elapsed:.1f}s)")
    return result


def run_pipeline(cfg: dict, steps: list[str] | None = None) -> None:
    all_steps = steps or list(STEPS.keys())

    print(f"Pipeline: {' → '.join(all_steps)}\n")

    if "ingest" in all_steps:
        print("[1/3] Ingest")
        run_step("build_universe", build_universe, cfg)
        run_step("ingest_prices", ingest_prices, cfg)

    if "transform" in all_steps:
        print("[2/3] Transform")
        run_step("normalize_ohlcv", normalize_ohlcv, cfg)

    if "load" in all_steps:
        print("[3/3] Load")
        run_step("load_duckdb", load_duckdb, cfg)

    print("\n✓ Pipeline complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Market Data Lake pipeline")
    parser.add_argument(
        "--step",
        choices=list(STEPS.keys()),
        help="Run a single pipeline step instead of the full pipeline",
    )
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to config file (default: config/config.yaml)",
    )
    args = parser.parse_args()

    cfg = load_cfg(args.config)

    steps = [args.step] if args.step else None
    run_pipeline(cfg, steps)


if __name__ == "__main__":
    main()
