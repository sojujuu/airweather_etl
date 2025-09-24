import pandas as pd
from etl.transform import clean_and_rename

def test_clean_and_rename_basic():
    df = pd.DataFrame({
        "tanggal":["2024-01-01","2024-01-02"],
        "TN":[25, None],
        "TX":[32, 33],
        "TAVG":[28, 29],
        "RH_AVG":[80, 82],
        "RR":[0.0, 1.2],
        "SS":[5.0, 6.0],
        "FF_X":[10, 12],
        "DDD_X":[180, 190],
        "FF_AVG":[5.5, 5.7],
        "stasiun":["DKI1", None],
        "pm25":[15, None],
        "pm10":[40, 42],
        "so2":[2, 2],
        "co":[0.5, 0.6],
        "o3":[10, 11],
        "no2":[7, 8],
        "max":[50, 55],
        "critical":["pm10", "pm10"],
        "categori":["SEDANG", "SEDANG"],
        "DDD_CAR":[1,2]
    })
    clean, bad = clean_and_rename(df)
    assert "ddd_car" not in clean.columns
    assert "suhu_min" in clean.columns
    assert clean.loc[1, "suhu_min"] == clean.loc[0,"suhu_min"]  # ffill works
