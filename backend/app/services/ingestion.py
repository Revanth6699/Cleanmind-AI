# app/services/ingestion.py

import os
import pandas as pd
from ..config import BASE_UPLOAD_DIR
from ..utils.id_gen import generate_dataset_id
from .ingestion_base import get_reader


def save_uploaded_file(file_obj) -> tuple[str, str]:
    """
    Save uploaded file to disk and return (dataset_id, file_path).
    """
    dataset_id = generate_dataset_id()

    # Get original extension (e.g. ".csv")
    _, ext = os.path.splitext(file_obj.filename or "")
    ext = ext.lower()

    if not ext:
        # default to .csv if no extension
        ext = ".csv"

    # Build path
    file_path = os.path.join(BASE_UPLOAD_DIR, f"{dataset_id}{ext}")

    # Write bytes to disk
    with open(file_path, "wb") as f:
        # FastAPI's UploadFile has .file
        f.write(file_obj.file.read())

    return dataset_id, file_path


def _find_file_by_dataset_id(dataset_id: str) -> str:
    """
    Locate the physical file on disk whose name starts with dataset_id.
    """
    for name in os.listdir(BASE_UPLOAD_DIR):
        if name.startswith(dataset_id):
            return os.path.join(BASE_UPLOAD_DIR, name)
    raise FileNotFoundError(f"Dataset {dataset_id} not found")


def load_dataset(dataset_id: str) -> pd.DataFrame:
    """
    Load a previously saved dataset as a pandas DataFrame,
    using the registered reader based on file extension.
    """
    file_path = _find_file_by_dataset_id(dataset_id)

    _, ext = os.path.splitext(file_path)
    ext = ext.lower().lstrip(".")

    reader = get_reader(ext)
    return reader(file_path)
def get_dataset_file_path(dataset_id: str) -> str:
    """
    Return the physical file path for a given dataset_id.
    Raises FileNotFoundError if not found.
    """
    for fname in os.listdir(BASE_UPLOAD_DIR):
        if fname.startswith(dataset_id):
            return os.path.join(BASE_UPLOAD_DIR, fname)
    raise FileNotFoundError(f"No dataset for id {dataset_id}")
