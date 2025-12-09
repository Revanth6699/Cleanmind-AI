import pandas as pd
from scipy.stats import zscore


def clean_dataset(df: pd.DataFrame):
    before_rows = len(df)
    before_missing = df.isna().sum().sum()

    # Missing values → median fill
    df_clean = df.copy()
    df_clean = df_clean.apply(lambda col: col.fillna(col.median()) if col.dtype != 'object' else col.fillna(col.mode()[0] if not col.mode().empty else ""))

    # Remove duplicates
    dup = df_clean.duplicated().sum()
    df_clean = df_clean.drop_duplicates()

    # Remove outliers numeric only
    num_cols = df_clean.select_dtypes(include=["float64", "int64"]).columns
    z_scores = (df_clean[num_cols].apply(zscore)).abs()
    outliers = (z_scores > 3).any(axis=1).sum()
    df_clean = df_clean.drop(df_clean[(z_scores > 3).any(axis=1)].index)

    after_rows = len(df_clean)
    after_missing = df_clean.isna().sum().sum()

    return df_clean, {
        "source_dataset_id": "",
        "n_rows_before": before_rows,
        "n_rows_after": after_rows,
        "n_missing_before": before_missing,
        "n_missing_after": after_missing,
        "duplicate_rows_removed": dup,
        "outlier_rows_removed": outliers
    }
