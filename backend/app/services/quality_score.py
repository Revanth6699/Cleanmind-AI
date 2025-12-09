# app/services/quality_score.py
import pandas as pd


def compute_quality_score(df: pd.DataFrame, dataset_id: str) -> dict:
    """
    Compute a simple overall data quality score from 0–100 based on:
    - missing values
    - duplicate rows
    - constant columns
    """
    n_rows, n_cols = df.shape

    if n_rows == 0 or n_cols == 0:
        metrics = {
            "missing_ratio": 1.0,
            "duplicate_ratio": 0.0,
            "constant_cols_ratio": 0.0,
        }
        return {
            "dataset_id": dataset_id,
            "quality_score": 0.0,
            "metrics": metrics,
        }

    total_cells = n_rows * n_cols

    missing_ratio = float(df.isna().sum().sum()) / float(total_cells)
    duplicate_ratio = float(df.duplicated().mean())
    constant_cols = [c for c in df.columns if df[c].nunique(dropna=True) <= 1]
    constant_cols_ratio = float(len(constant_cols)) / float(n_cols)

    # weighted penalty, then convert to 0–100
    penalty = missing_ratio * 0.6 + duplicate_ratio * 0.3 + constant_cols_ratio * 0.1
    score = 100.0 * max(0.0, 1.0 - penalty)

    metrics = {
        "missing_ratio": missing_ratio,
        "duplicate_ratio": duplicate_ratio,
        "constant_cols_ratio": constant_cols_ratio,
    }

    return {
        "dataset_id": dataset_id,
        "quality_score": score,
        "metrics": metrics,
    }
    null_pct = df.isnull().mean().mean() * 100

    quality_score = 100 - null_pct   # Very basic scoring

    return {
        "dataset_id": dataset_id,
        "quality_score": round(quality_score, 2),
        "metrics": {
            "missing_percentage": round(null_pct, 2),
            "column_count": df.shape[1],
            "row_count": df.shape[0]
        }
    }
