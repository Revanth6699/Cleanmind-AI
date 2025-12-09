# app/services/profiling.py
import pandas as pd


def generate_profile(df: pd.DataFrame, dataset_id: str) -> dict:
    """
    Build a simple profile for each column:
    - dtype
    - number & % of missing values
    - number of unique values
    - a few sample values
    """
    n_rows, n_cols = df.shape

    columns_profile = {}
    for col in df.columns:
        s = df[col]
        n_missing = int(s.isna().sum())
        pct_missing = float(s.isna().mean() * 100.0)
        n_unique = int(s.nunique(dropna=True))
        sample_values = [str(v) for v in s.dropna().unique()[:5]]

        columns_profile[col] = {
            "dtype": str(s.dtype),
            "n_missing": n_missing,
            "pct_missing": pct_missing,
            "n_unique": n_unique,
            "sample_values": sample_values,
        }

    return {
        "dataset_id": dataset_id,
        "n_rows": int(n_rows),
        "n_cols": int(n_cols),
        "columns": columns_profile,
    }
# backend/app/services/profiling.py
def get_profile(df, dataset_id):
    return {
        "dataset_id": dataset_id,
        "n_rows": len(df),
        "n_cols": len(df.columns),
        "columns": {col: str(df[col].dtype) for col in df.columns}
    }
