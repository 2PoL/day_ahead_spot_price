#!/usr/bin/env python
"""
marginal_model_workflow.py
==========================
One‑click incremental pipeline for pwlf marginal‑price modelling.

Changelog (2025‑04‑25)
----------------------
* **Robust numeric validation** — automatically drops rows where the X or Y
  column is missing / non‑finite to avoid *ValueError: bounds should be a
  sequence containing finite real valued ...* raised by SciPy.
* Clear warning when the remaining sample size is too small for the requested
  segment count.
* Minor: stronger typing & tidy imports.
"""
from __future__ import annotations

import argparse
import pickle
import warnings
from pathlib import Path
from typing import Tuple

import numpy as np
import pandas as pd
import pwlf

# ──────────────────────────────────────────────────────────────────────────────
# I/O helpers
# ──────────────────────────────────────────────────────────────────────────────

def load_dataframe(path: Path, date_col: str) -> pd.DataFrame:
    """Load CSV/XLSX, parse *date_col* as datetime, return DataFrame."""
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_excel(path) if path.suffix.lower() in {".xls", ".xlsx"} else pd.read_csv(path)
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    return df


def save_dataframe(df: pd.DataFrame, path: Path) -> None:  # noqa: D401
    "Save DataFrame preserving extension (csv / xlsx)."  # single‑line docstring to appease pydocstyle
    if path.suffix.lower() == ".csv":
        df.to_csv(path, index=False)
    else:
        df.to_excel(path, index=False)


# ──────────────────────────────────────────────────────────────────────────────
# Data cleaning helpers
# ──────────────────────────────────────────────────────────────────────────────

def _clean_numeric(df: pd.DataFrame, x_col: str, y_col: str) -> pd.DataFrame:
    """Ensure *x_col* & *y_col* are numeric and finite; drop bad rows."""
    for col in (x_col, y_col):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    mask = np.isfinite(df[x_col]) & np.isfinite(df[y_col])
    dropped = (~mask).sum()
    if dropped:
        warnings.warn(f"Dropped {dropped} rows with non‑finite {x_col}/{y_col}")
    return df.loc[mask].copy()


# ──────────────────────────────────────────────────────────────────────────────
# Outlier detection
# ──────────────────────────────────────────────────────────────────────────────

def _modified_z_score(residuals: np.ndarray) -> np.ndarray:
    med = np.median(residuals)
    mad = np.median(np.abs(residuals - med)) or 1e-12
    return 0.6745 * (residuals - med) / mad


def flag_outliers(model: "pwlf.PiecewiseLinFit", df: pd.DataFrame, *,
                  x_col: str, y_col: str, z_thresh: float) -> pd.Series:
    z = np.abs(_modified_z_score(df[y_col].values - model.predict(df[x_col].values)))
    return z > z_thresh


# ──────────────────────────────────────────────────────────────────────────────
# Model training
# ──────────────────────────────────────────────────────────────────────────────

def train_pwlf(df: pd.DataFrame, *, x_col: str, y_col: str, n_segments: int,
               w_low: float, w_high: float) -> "pwlf.PiecewiseLinFit":
    df = _clean_numeric(df, x_col, y_col)
    if df.shape[0] <= n_segments:
        raise ValueError(
            f"Need > {n_segments} samples after cleaning; only {df.shape[0]} left.")

    x, y = df[x_col].values, df[y_col].values
    w = np.ones_like(x)
    w[x <= w_low] = 3.0
    w[x >= w_high] = 3.0

    model = pwlf.PiecewiseLinFit(x, y, weights=w)
    model.fit(n_segments)
    return model


# ──────────────────────────────────────────────────────────────────────────────
# Pipeline driver
# ──────────────────────────────────────────────────────────────────────────────

def update_pipeline(historic_path: Path, new_path: Path, model_path: Path, *,
                    date_col: str, x_col: str, y_col: str, n_segments: int,
                    w_low: float, w_high: float, z_thresh: float
                    ) -> Tuple[pd.DataFrame, "pwlf.PiecewiseLinFit", pd.DataFrame]:
    hist_df = _clean_numeric(load_dataframe(historic_path, date_col), x_col, y_col)
    new_df_raw = _clean_numeric(load_dataframe(new_path, date_col), x_col, y_col)

    if model_path.exists():
        with open(model_path, "rb") as f:
            model = pickle.load(f)
    else:
        model = train_pwlf(hist_df, x_col=x_col, y_col=y_col,
                           n_segments=n_segments, w_low=w_low, w_high=w_high)

    outlier_mask = flag_outliers(model, new_df_raw, x_col=x_col, y_col=y_col,
                                 z_thresh=z_thresh)
    clean_new_df, rejected_df = new_df_raw[~outlier_mask], new_df_raw[outlier_mask]

    combined_df = pd.concat([hist_df, clean_new_df], ignore_index=True)
    new_model = train_pwlf(combined_df, x_col=x_col, y_col=y_col,
                           n_segments=n_segments, w_low=w_low, w_high=w_high)

    save_dataframe(combined_df, historic_path)
    with open(model_path, "wb") as f:
        pickle.dump(new_model, f)
    if not rejected_df.empty:
        save_dataframe(rejected_df, model_path.with_name(model_path.stem + "_rejected.csv"))

    return combined_df, new_model, rejected_df


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

def _parse_args():
    p = argparse.ArgumentParser("Incrementally update pwlf marginal model")
    p.add_argument("--historic", required=True, type=Path)
    p.add_argument("--new", required=True, type=Path)
    p.add_argument("--model", required=True, type=Path)
    p.add_argument("--segments", type=int, default=3)
    p.add_argument("--date-col", default="date")
    p.add_argument("--x-col", default="load_rate")
    p.add_argument("--y-col", default="price")
    p.add_argument("--w-low", type=float, default=0.6)
    p.add_argument("--w-high", type=float, default=0.92)
    p.add_argument("--z-thresh", type=float, default=3.5)
    return p.parse_args()


def main():
    args = _parse_args()

    combined_df, new_model, rejected_df = update_pipeline(
        historic_path=args.historic,
        new_path=args.new,
        model_path=args.model,
        date_col=args.date_col,
        x_col=args.x_col,
        y_col=args.y_col,
        n_segments=args.segments,
        w_low=args.w_low,
        w_high=args.w_high,
        z_thresh=args.z_thresh,
    )
    print(f"Updated dataset rows : {combined_df.shape[0]}")
    print(f"Rejected new rows    : {rejected_df.shape[0]}")
    print("Break points         :", new_model.fit_breaks)
    print("Betas                :", new_model.beta)


if __name__ == "__main__":
    main()
