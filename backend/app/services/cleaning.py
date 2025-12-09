# app/services/cleaning.py

import os
import numpy as np
import pandas as pd

from ..config import BASE_UPLOAD_DIR
from ..utils.id_gen import generate_dataset_id
from .ingestion import load_dataset
from ..schemas.datasets import CleaningOptions


def _impute_column(s: pd.Series, strategy: str) -> pd.Series:
    if not s.isna().any():
        return s

    if strategy == "zero":
        fill_value = 0
    elif strategy == "mode":
        fill_value = s.mode(dropna=True)
        fill_value = fill_value.iloc[0] if not fill_value.empty else 0
    else:
        # numeric only for mean/median; others use mode
        if pd.api.types.is_numeric_dtype(s):
            if strategy == "mean":
                fill_value = s.mean()
            else:  # median
                fill_value = s.median()
        else:
            fill_value = s.mode(dropna=True)
            fill_value = fill_value.iloc[0] if not fill_value.empty else ""
    return s.fillna(fill_value)


def _remove_outliers_zscore(df: pd.DataFrame, z_thresh: float) -> tuple[pd.DataFrame, int]:
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if numeric_cols.empty:
        return df, 0

    zscores = (df[numeric_cols] - df[numeric_cols].mean()) / df[numeric_cols].std(ddof=0)
    mask = (zscores.abs() > z_thresh).any(axis=1)

    outliers_count = int(mask.sum())
    df_no_outliers = df.loc[~mask].copy()
    return df_no_outliers, outliers_count


def clean_dataset(dataset_id: str, options: CleaningOptions):
    """
    Load a dataset, clean it according to the options, save cleaned version as new dataset.
    Returns: (cleaned_dataset_id, summary_dict)
    """
    df = load_dataset(dataset_id)

    n_rows_before = int(df.shape[0])
    n_missing_before = int(df.isna().sum().sum())

    # 1) Drop duplicates
    dup_rows_before = int(df.duplicated().sum())
    if options.drop_duplicates:
        df = df.drop_duplicates().reset_index(drop=True)
    dup_rows_after = int(df.duplicated().sum())
    duplicate_rows_removed = dup_rows_before - dup_rows_after

    # 2) Impute missing
    if options.impute_missing:
        for col in df.columns:
            df[col] = _impute_column(df[col], options.impute_strategy)

    # 3) Remove outliers
    outlier_rows_removed = 0
    if options.remove_outliers:
        df, outlier_rows_removed = _remove_outliers_zscore(df, options.outlier_zscore_threshold)

    n_rows_after = int(df.shape[0])
    n_missing_after = int(df.isna().sum().sum())

    # Save cleaned dataset as CSV with a new id
    cleaned_dataset_id = generate_dataset_id()
    file_path = os.path.join(BASE_UPLOAD_DIR, f"{cleaned_dataset_id}.csv")
    df.to_csv(file_path, index=False)

    preview = df.head(20).fillna("").astype(str).to_dict(orient="records")

    summary = {
        "source_dataset_id": dataset_id,
        "cleaned_dataset_id": cleaned_dataset_id,
        "n_rows_before": n_rows_before,
        "n_rows_after": n_rows_after,
        "n_missing_before": n_missing_before,
        "n_missing_after": n_missing_after,
        "duplicate_rows_removed": duplicate_rows_removed,
        "outlier_rows_removed": outlier_rows_removed,
        "preview_rows": preview,
    }

    return cleaned_dataset_id, summary
