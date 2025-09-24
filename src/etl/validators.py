import os
import pandas as pd
from .config import REQUIRED_WEATHER_COLS, REQUIRED_ISPU_COLS

class ValidationError(Exception): ...
class MissingColumnsError(ValidationError): ...

def ensure_files_exist(incoming_dir: str, weather_name: str, ispu_name: str):
    weather_path = os.path.join(incoming_dir, weather_name)
    ispu_path = os.path.join(incoming_dir, ispu_name)
    errs = []
    if not os.path.isfile(weather_path): errs.append(f"Missing file: {weather_path}")
    if not os.path.isfile(ispu_path): errs.append(f"Missing file: {ispu_path}")
    if errs:
        raise ValidationError("; ".join(errs))
    return weather_path, ispu_path

def infer_city_from_filename(filename: str) -> str:
    # city is the last token before .csv; allow patterns like *_jakarta.csv
    base = os.path.basename(filename)
    if not base.lower().endswith(".csv"):
        raise ValidationError(f"Not a CSV: {filename}")
    name = base[:-4]  # strip .csv
    city = name.split("_")[-1].strip()
    if not city:
        raise ValidationError(f"Cannot infer city from filename: {filename}")
    return city

def validate_csv_columns(path: str, required_cols: list[str], allow_extra=True, sep=","):
    try:
        sample = pd.read_csv(path, nrows=5, sep=sep)
    except Exception as e:
        raise ValidationError(f"Failed reading CSV {path}: {e}")
    cols = list(sample.columns)
    missing = [c for c in required_cols if c not in cols]
    if missing:
        raise MissingColumnsError(f"{os.path.basename(path)} missing columns: {missing}")
    return True

def read_csv_full(path: str, sep=",") -> pd.DataFrame:
    return pd.read_csv(path, sep=sep)
