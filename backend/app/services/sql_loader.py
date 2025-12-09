# app/services/sql_loader.py

import os
import sqlite3
import pandas as pd

from ..utils.id_gen import generate_dataset_id
from ..config import BASE_UPLOAD_DIR


def load_from_sqlite(db_path: str, query: str) -> tuple[str, str]:
    """
    Load data from a SQLite database using a SQL query,
    save it as CSV into the uploads folder, and return (dataset_id, file_path).
    """
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    dataset_id = generate_dataset_id()
    file_path = os.path.join(BASE_UPLOAD_DIR, f"{dataset_id}.csv")
    df.to_csv(file_path, index=False)

    return dataset_id, file_path
