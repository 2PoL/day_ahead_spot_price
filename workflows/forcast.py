#!/usr/bin/env python
"""
predict_price.py
================
Command‑line helper to **predict day‑ahead clearing price** from load‑rate
using a previously trained pwlf (PiecewiseLinFit) model.

Typical usage
-------------
1. **Scalar or list of values**::

       python predict_price.py --model model/pwlf_model.pkl --x 0.55 0.78 0.93

2. **Batch CSV** (adds a new column and writes result)::

       python predict_price.py --model model/pwlf_model.pkl \
              --csv data/new_load_rates.csv --out data/prices_pred.csv

By default the script expects the X column in the CSV to be named
``日前负荷率(%)`` but you can override with ``--x-col``.
"""
from __future__ import annotations

import argparse
import pickle
import sys
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_model(path: Path):
    if not path.exists():
        sys.exit(f"Model file not found: {path}")
    with open(path, "rb") as f:
        return pickle.load(f)


def _predict(model, x: np.ndarray) -> np.ndarray:
    """Vectorised safe prediction."""
    return model.predict(x)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args():
    p = argparse.ArgumentParser("Predict price from load‑rate using pwlf model")

    p.add_argument("--model", required=True, type=Path, help="Path to pwlf_model.pkl")

    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--x", nargs="+", type=float,
                   help="One or more load‑rate values (e.g. 0.6 0.75 0.95)")
    g.add_argument("--csv", type=Path,
                   help="CSV/XLSX file containing load‑rate column to predict")

    p.add_argument("--x-col", default="load_rate",
                   help="Column name to read load‑rate from if using --csv")
    p.add_argument("--out", type=Path,
                   help="Optional output file (csv/xlsx). If omitted prints to stdout")

    return p.parse_args()


def main():
    args = _parse_args()
    model = _load_model(args.model)

    if args.x is not None:
        x_arr = np.array(args.x, dtype=float)
        y_pred = _predict(model, x_arr)
        for x_val, y_val in zip(x_arr, y_pred):
            print(f"load‑rate={x_val:.4f}  →  price={y_val:.2f}")
        return

    # CSV/XLSX path
    if not args.csv.exists():
        sys.exit(f"Input file not found: {args.csv}")

    # Read file and ensure column exists
    in_df: pd.DataFrame
    if args.csv.suffix.lower() in {".xls", ".xlsx"}:
        in_df = pd.read_excel(args.csv)
    else:
        in_df = pd.read_csv(args.csv)

    if args.x_col not in in_df.columns:
        sys.exit(f"Column '{args.x_col}' not found in {args.csv}")

    # Compute prediction
    x_values = pd.to_numeric(in_df[args.x_col], errors="coerce")
    nan_mask = ~np.isfinite(x_values)
    if nan_mask.any():
        print(f"Warning: {nan_mask.sum()} rows with NaN/inf {args.x_col} ignored", file=sys.stderr)
    y_pred = _predict(model, x_values.fillna(0).values)  # fillna to match length

    out_df = in_df.copy()
    out_df["predicted_price"] = y_pred

    if args.out:
        if args.out.suffix.lower() == ".csv":
            out_df.to_csv(args.out, index=False)
        else:
            out_df.to_excel(args.out, index=False)
        print(f"Saved predictions to {args.out}")
    else:
        print(out_df[[args.x_col, "predicted_price"]].to_string(index=False))


if __name__ == "__main__":
    main()
