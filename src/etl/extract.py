import pandas as pd
from .validators import read_csv_full

def extract_weather(path: str) -> pd.DataFrame:
    df = read_csv_full(path)
    return df

def extract_ispu(path: str) -> pd.DataFrame:
    df = read_csv_full(path)
    return df

def merge_outer_by_date(df_weather: pd.DataFrame, df_ispu: pd.DataFrame) -> pd.DataFrame:
    # normalize date column names to 'tanggal' for merge
    dfw = df_weather.rename(columns={"TANGGAL":"tanggal"})
    dfi = df_ispu.rename(columns={"Tanggal":"tanggal"})
    merged = pd.merge(dfw, dfi, on="tanggal", how="outer", suffixes=("_w","_i"))
    merged = merged.sort_values("tanggal").reset_index(drop=True)
    return merged
