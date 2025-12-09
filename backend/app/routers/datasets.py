# backend/app/routers/datasets.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel
from uuid import uuid4

import numpy as np
import pandas as pd

router = APIRouter()

# Paths
BASE_DIR = Path(__file__).resolve().parents[2]  # backend/
DATA_ROOT = BASE_DIR / "uploaded_datasets"
DATA_ROOT.mkdir(exist_ok=True)


# ========= Models =========
class ColumnProfile(BaseModel):
    dtype: str
    n_missing: int
    pct_missing: float
    n_unique: int
    sample_values: List[str]


class DatasetProfileResponse(BaseModel):
    dataset_id: str
    n_rows: int
    n_cols: int
    columns: Dict[str, ColumnProfile]


class QualityScoreResponse(BaseModel):
    dataset_id: str
    quality_score: float
    metrics: Dict[str, float]


class CleaningResult(BaseModel):
    source_dataset_id: str
    cleaned_dataset_id: str
    n_rows_before: int
    n_rows_after: int
    n_missing_before: int
    n_missing_after: int
    duplicate_rows_removed: int
    outlier_rows_removed: int
    preview_rows: List[Dict[str, Any]]


# ========= Helpers =========
def _dataset_path(dataset_id: str) -> Path:
    """All datasets (raw + cleaned) are stored as CSV with this id."""
    return DATA_ROOT / f"{dataset_id}.csv"


def _load_dataset(dataset_id: str) -> pd.DataFrame:
    path = _dataset_path(dataset_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Dataset not found")
    return pd.read_csv(path)


def _save_dataset(df: pd.DataFrame, dataset_id: str) -> Path:
    path = _dataset_path(dataset_id)
    df.to_csv(path, index=False)
    return path


def _read_upload_to_df(file: UploadFile) -> pd.DataFrame:
    name = file.filename or "uploaded"
    ext = name.rsplit(".", 1)[-1].lower()

    try:
        if ext in ("csv", "txt"):
            return pd.read_csv(file.file)
        elif ext in ("xlsx", "xls", "xlsm", "ods"):
            return pd.read_excel(file.file)
        elif ext in ("json", "jsonl", "ndjson"):
            return pd.read_json(file.file, lines=ext in ("jsonl", "ndjson"))
        elif ext in ("parquet",):
            return pd.read_parquet(file.file)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type: .{ext}. "
                f"Use CSV / Excel / JSON / Parquet.",
            )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {e}") from e


def _profile_dataframe(df: pd.DataFrame, dataset_id: str) -> DatasetProfileResponse:
    n_rows, n_cols = df.shape
    cols: Dict[str, ColumnProfile] = {}

    for col in df.columns:
        series = df[col]
        n_missing = int(series.isna().sum())
        pct_missing = float(n_missing / len(series)) * 100 if len(series) else 0.0
        n_unique = int(series.nunique(dropna=True))
        sample_vals = (
            series.dropna().astype(str).head(5).tolist()
            if len(series.dropna()) > 0
            else []
        )

        cols[col] = ColumnProfile(
            dtype=str(series.dtype),
            n_missing=n_missing,
            pct_missing=pct_missing,
            n_unique=n_unique,
            sample_values=sample_vals,
        )

    return DatasetProfileResponse(
        dataset_id=dataset_id,
        n_rows=int(n_rows),
        n_cols=int(n_cols),
        columns=cols,
    )


def _quality_score(df: pd.DataFrame, dataset_id: str) -> QualityScoreResponse:
    total_cells = df.shape[0] * df.shape[1] if df.size else 1
    missing_ratio = float(df.isna().sum().sum() / total_cells)

    duplicate_ratio = float(df.duplicated().mean()) if len(df) > 0 else 0.0

    nunique = df.nunique(dropna=False)
    constant_cols_ratio = float((nunique == 1).sum() / max(len(nunique), 1))

    # Simple scoring formula – you can tweak weights
    score = 100.0
    score -= missing_ratio * 40.0
    score -= duplicate_ratio * 30.0
    score -= constant_cols_ratio * 30.0
    score = max(0.0, min(100.0, score))

    metrics = {
        "missing_ratio": missing_ratio,
        "duplicate_ratio": duplicate_ratio,
        "constant_cols_ratio": constant_cols_ratio,
    }

    return QualityScoreResponse(dataset_id=dataset_id, quality_score=score, metrics=metrics)


def _clean_dataframe(df: pd.DataFrame):
    n_rows_before = len(df)
    n_missing_before = int(df.isna().sum().sum())

    # 1) drop duplicates
    duplicate_rows_removed = int(df.duplicated().sum())
    df = df.drop_duplicates()

    # 2) impute missing
    num_cols = df.select_dtypes(include=["number"]).columns.tolist()
    cat_cols = [c for c in df.columns if c not in num_cols]

    for col in num_cols:
        median = df[col].median()
        df[col] = df[col].fillna(median)

    for col in cat_cols:
        mode = df[col].mode(dropna=True)
        fill_value = mode.iloc[0] if not mode.empty else ""
        df[col] = df[col].fillna(fill_value)

    # 3) remove numeric outliers using z-score > 3
    outlier_rows_removed = 0
    if num_cols:
        z = np.abs(
            (df[num_cols] - df[num_cols].mean())
            / df[num_cols].std(ddof=0).replace(0, np.nan)
        )
        outlier_mask = (z > 3).any(axis=1)
        outlier_rows_removed = int(outlier_mask.sum())
        df = df.loc[~outlier_mask].copy()

    n_rows_after = len(df)
    n_missing_after = int(df.isna().sum().sum())

    return (
        df,
        n_rows_before,
        n_rows_after,
        n_missing_before,
        n_missing_after,
        duplicate_rows_removed,
        outlier_rows_removed,
    )


# ========= Routes =========
@router.post("/upload", response_model=dict)
async def upload_dataset(file: UploadFile = File(...)):
    """
    Upload a CSV / Excel / JSON / Parquet file.
    It is parsed with pandas and stored internally as CSV.
    """
    df = _read_upload_to_df(file)
    dataset_id = str(uuid4())
    _save_dataset(df, dataset_id)
    return {"dataset_id": dataset_id}


@router.get("/{dataset_id}/profile", response_model=DatasetProfileResponse)
async def get_dataset_profile(dataset_id: str):
    df = _load_dataset(dataset_id)
    return _profile_dataframe(df, dataset_id)


@router.get("/{dataset_id}/quality_score", response_model=QualityScoreResponse)
async def get_quality_score(dataset_id: str):
    df = _load_dataset(dataset_id)
    return _quality_score(df, dataset_id)


@router.post("/{dataset_id}/clean", response_model=CleaningResult)
async def clean_dataset(dataset_id: str):
    df = _load_dataset(dataset_id)
    (
        cleaned_df,
        n_rows_before,
        n_rows_after,
        n_missing_before,
        n_missing_after,
        duplicate_rows_removed,
        outlier_rows_removed,
    ) = _clean_dataframe(df)

    cleaned_id = str(uuid4())
    _save_dataset(cleaned_df, cleaned_id)

    preview_rows = (
        cleaned_df.head(20).astype(str).to_dict(orient="records")  # small preview
    )

    return CleaningResult(
        source_dataset_id=dataset_id,
        cleaned_dataset_id=cleaned_id,
        n_rows_before=n_rows_before,
        n_rows_after=n_rows_after,
        n_missing_before=n_missing_before,
        n_missing_after=n_missing_after,
        duplicate_rows_removed=duplicate_rows_removed,
        outlier_rows_removed=outlier_rows_removed,
        preview_rows=preview_rows,
    )


@router.get("/{dataset_id}/download")
async def download_dataset(dataset_id: str):
    """
    Download the (raw or cleaned) dataset as CSV.
    """
    path = _dataset_path(dataset_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Dataset not found")
    return FileResponse(
        path,
        media_type="text/csv",
        filename=f"{dataset_id}.csv",
    )
