# app/schemas/datasets.py
from pydantic import BaseModel
from typing import Dict, List, Optional
from pydantic import BaseModel
from typing import Literal



class UploadResponse(BaseModel):
    dataset_id: str


class ColumnProfile(BaseModel):
    dtype: str
    n_missing: int
    pct_missing: float
    n_unique: int
    sample_values: Optional[List[str]] = None


class DatasetProfileResponse(BaseModel):
    dataset_id: str
    n_rows: int
    n_cols: int
    columns: Dict[str, ColumnProfile]


class QualityScoreResponse(BaseModel):
    dataset_id: str
    quality_score: float
    metrics: Dict[str, float]
    
class CleaningOptions(BaseModel):
    drop_duplicates: bool = True
    impute_missing: bool = True
    impute_strategy: Literal["mean", "median", "mode", "zero"] = "median"
    remove_outliers: bool = True
    outlier_zscore_threshold: float = 3.0


class CleaningResult(BaseModel):
    source_dataset_id: str
    cleaned_dataset_id: str
    n_rows_before: int
    n_rows_after: int
    n_missing_before: int
    n_missing_after: int
    duplicate_rows_removed: int
    outlier_rows_removed: int
    preview_rows: List[Dict[str, Optional[str]]]  # first few rows of cleaned data

class SQLiteIngestRequest(BaseModel):
    db_path: str   # e.g. 'C:/Users/.../mydb.sqlite'
    query: str     # e.g. 'SELECT * FROM my_table'
