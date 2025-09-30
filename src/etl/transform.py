import pandas as pd
import numpy as np
from .config import RENAME_MAP

def _coerce_date_yyyy_mm_dd(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    parsed = pd.to_datetime(series, errors="coerce", dayfirst=False)
    # produce normalized string date
    norm = parsed.dt.strftime("%Y-%m-%d")
    return norm, parsed.isna()

def normalize_special_missing(df: pd.DataFrame) -> pd.DataFrame:
    SPECIAL_MISSING = {"8888","9999","-999","-9999","na","n/a","null","none",""," "}
    df = df.copy()
    df = df.replace(list(SPECIAL_MISSING), np.nan)
    return df

def fill_missing_ffill_bfill(df: pd.DataFrame, cols: list[str]):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].ffill().bfill()
    return df

def clean_and_rename(df_airweather: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df_airweather.copy()
    
    # 0) Special missing handling
    df = normalize_special_missing(df)

    # 1) Coerce tanggal
    df["tanggal"], bad = _coerce_date_yyyy_mm_dd(df["tanggal"])
    bad_rows = df[bad].copy()
    df = df[~bad].copy()

    # 2) Apply ffill/bfill to all specified columns (case-insensitive handling)
    cols_to_ffill = [
        "TN","TX","TAVG","RH_AVG","RR","SS","FF_X","DDD_X","FF_AVG",
        "stasiun","pm25","pm10","so2","co","o3","no2","max","critical","categori"
    ]
    # unify column access by exact names present
    present = [c for c in cols_to_ffill if c in df.columns]
    for c in present:
        df[c] = df[c].ffill().bfill()

    # 3) Downcase all column names to snake_case
    def to_snake(name: str) -> str:
        return name.lower()

    df.columns = [to_snake(c) for c in df.columns]

    # 4) Drop ddd_car if present
    if "ddd_car" in df.columns:
        df = df.drop(columns=["ddd_car"])

    # 5) Rename per mapping
    rename_keys = {k.lower(): v for k,v in RENAME_MAP.items()}
    df = df.rename(columns=rename_keys)

    # Ensure numeric columns are numeric
    num_cols = ["suhu_min","suhu_max","suhu_avg","kelembapan_avg","curah_hujan",
                "durasi_penyinaran","kecepatan_angin_max","arah_angin_max","kecepatan_angin_avg",
                "pm25","pm10","so2","co","o3","no2","max"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df, bad_rows
