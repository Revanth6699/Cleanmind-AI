# app/services/file_readers.py
import json
import pandas as pd
import yaml
from scipy.io import loadmat

from .ingestion_base import register_reader


# -------------------------------------------------------------------
# Helper: robust CSV reader that tries multiple encodings
# -------------------------------------------------------------------
def _read_csv_with_encodings(path: str, **kwargs) -> pd.DataFrame:
    """
    Try several common encodings so Windows / Excel CSVs don't break.
    """
    encodings_to_try = ["utf-8", "utf-8-sig", "cp1252", "latin1"]
    last_err = None

    for enc in encodings_to_try:
        try:
            return pd.read_csv(path, encoding=enc, **kwargs)
        except UnicodeDecodeError as e:
            last_err = e
            continue

    # If we get here, all encodings failed
    raise ValueError(
        f"Could not decode text file due to encoding issues. "
        f"Tried encodings {encodings_to_try}. Last error: {last_err}"
    )


# --------- BASIC TABULAR: CSV / TSV / TXT / ODS / EXCEL ----------

@register_reader(["csv"])
def read_csv(path: str) -> pd.DataFrame:
    # Plain CSV – robust encoding handling
    return _read_csv_with_encodings(path)


@register_reader(["tsv"])
def read_tsv(path: str) -> pd.DataFrame:
    # Tab-separated
    return _read_csv_with_encodings(path, sep="\t")


@register_reader(["txt", "log"])
def read_txt(path: str) -> pd.DataFrame:
    # Try to auto-detect delimiter, with robust encodings
    return _read_csv_with_encodings(path, sep=None, engine="python")


@register_reader(["xlsx", "xls"])
def read_excel(path: str) -> pd.DataFrame:
    """
    Excel reader. Requires 'openpyxl'.
    """
    try:
        # pandas will use openpyxl; for old .xls this is usually fine too
        return pd.read_excel(path, engine="openpyxl")
    except ImportError as e:
        # This is the error you saw: "Missing optional dependency 'openpyxl'"
        raise ValueError(
            "Missing Excel dependency 'openpyxl'. "
            "Install it in the backend environment with: pip install openpyxl"
        ) from e


@register_reader(["ods"])
def read_ods(path: str) -> pd.DataFrame:
    """
    ODS reader. Requires 'odfpy'.
    """
    try:
        # pandas can read ODS via engine="odf"
        return pd.read_excel(path, engine="odf")
    except ImportError as e:
        raise ValueError(
            "Missing ODS dependency 'odfpy'. "
            "Install it in the backend environment with: pip install odfpy"
        ) from e


# --------- JSON FAMILY: JSON / JSONL / NDJSON / GEOJSON ----------

@register_reader(["json"])
def read_json(path: str) -> pd.DataFrame:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return pd.json_normalize(data)


@register_reader(["jsonl", "ndjson"])
def read_jsonl(path: str) -> pd.DataFrame:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))
    return pd.json_normalize(rows)


@register_reader(["geojson"])
def read_geojson(path: str) -> pd.DataFrame:
    import geopandas as gpd

    gdf = gpd.read_file(path)
    # drop geometry so we return a pure tabular frame
    return pd.DataFrame(gdf.drop(columns="geometry"))


# --------- XML / HTML / YAML ----------

@register_reader(["xml"])
def read_xml(path: str) -> pd.DataFrame:
    # Simple XML -> DataFrame using pandas where possible
    try:
        return pd.read_xml(path)
    except Exception:
        # Fallback manual parser
        from xml import etree

        tree = etree.parse(path)
        root = tree.getroot()
        rows = []
        for elem in root:
            row = {child.tag: child.text for child in elem}
            rows.append(row)
        return pd.DataFrame(rows)


@register_reader(["html", "htm"])
def read_html(path: str) -> pd.DataFrame:
    tables = pd.read_html(path)
    return tables[0] if tables else pd.DataFrame()


@register_reader(["yaml", "yml"])
def read_yaml(path: str) -> pd.DataFrame:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return pd.json_normalize(data)


# --------- COLUMNAR / BINARY TABULAR: PARQUET / FEATHER / ORC / HDF5 / MAT ----------

@register_reader(["parquet"])
def read_parquet(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)


@register_reader(["feather"])
def read_feather(path: str) -> pd.DataFrame:
    return pd.read_feather(path)


@register_reader(["orc"])
def read_orc(path: str) -> pd.DataFrame:
    import pyarrow.orc as orc

    with open(path, "rb") as f:
        data = orc.ORCFile(f)
        return data.read().to_pandas()


@register_reader(["hdf5", "h5"])
def read_hdf(path: str) -> pd.DataFrame:
    # Needs key; for now, assume the first key
    store = pd.HDFStore(path)
    try:
        key = store.keys()[0]
        return store[key]
    finally:
        store.close()


@register_reader(["mat"])
def read_mat(path: str) -> pd.DataFrame:
    mat = loadmat(path)
    # choose one variable to convert; here we just take the first 2D array-like
    for key, value in mat.items():
        if (
            not key.startswith("__")
            and hasattr(value, "shape")
            and len(value.shape) == 2
        ):
            return pd.DataFrame(value)
    raise ValueError("No 2D array found in MAT file")
