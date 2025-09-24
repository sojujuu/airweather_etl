from typing import Dict, List, Tuple
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from .db import get_engine, fetch_scalar, fetch_one
from .logging_util import get_logger

logger = get_logger(__name__)

def _get_city_id(engine: Engine, city_name: str) -> int:
    sql = """
    SELECT city_id FROM city WHERE LOWER(name)=LOWER(:name) LIMIT 1
    """
    cid = fetch_scalar(engine, sql, {"name": city_name})
    if cid is None:
        raise RuntimeError(f"City '{city_name}' not found in CITY table.")
    return int(cid)

def _get_location_id_for_city(engine: Engine, city_id: int) -> int:
    # pick the first location for that city (or you can customize to choose by station_code)
    sql = """
    SELECT location_id FROM location WHERE city_id=:cid ORDER BY location_id LIMIT 1
    """
    lid = fetch_scalar(engine, sql, {"cid": city_id})
    if lid is None:
        raise RuntimeError(f"No location found for city_id={city_id}. Populate 'location' first.")
    return int(lid)

def _get_weatherattr_ids(engine: Engine) -> Dict[str,int]:
    rows = engine.connect().execute(text("SELECT weatherattr_id, weatherattr_code FROM weather_attribute")).mappings().all()
    return {r["weatherattr_code"].lower(): int(r["weatherattr_id"]) for r in rows}

def _get_pollutantattr_ids(engine: Engine) -> Dict[str,int]:
    rows = engine.connect().execute(text("SELECT pollutantattr_id, pollutantattr_code FROM pollutant_attribute")).mappings().all()
    return {r["pollutantattr_code"].lower(): int(r["pollutantattr_id"]) for r in rows}

def insert_weather_and_pollutants(engine: Engine, location_id: int, df: pd.DataFrame):
    wmap = _get_weatherattr_ids(engine)
    pmap = _get_pollutantattr_ids(engine)

    weather_pairs = [
        ("suhu_min","suhu_min"),
        ("suhu_max","suhu_max"),
        ("suhu_avg","suhu_avg"),
        ("kelembapan_avg","kelembapan_avg"),
        ("curah_hujan","curah_hujan"),
        ("durasi_penyinaran","durasi_penyinaran"),
        ("kecepatan_angin_max","kecepatan_angin_max"),
        ("arah_angin_max","arah_angin_max"),
        ("kecepatan_angin_avg","kecepatan_angin_avg"),
    ]

    pollutant_cols = ["pm25","pm10","so2","co","o3","no2"]

    with engine.begin() as conn:
        # WEATHER
        w_stmt = text("""
            INSERT IGNORE INTO weather_observation (location_id, weatherobs_date, weatherattr_id, weatherobs_value)
            VALUES (:loc, :dt, :attr, :val)
        """)
        for _, row in df.iterrows():
            dt = row["tanggal"]
            for code, col in weather_pairs:
                if col in df.columns and code in wmap:
                    conn.execute(w_stmt, {"loc": location_id, "dt": dt, "attr": wmap[code], "val": row.get(col)})

        # POLLUTANTS
        p_stmt = text("""
            INSERT IGNORE INTO pollutant_observation (location_id, pollobs_date, pollutantattr_id, pollobs_value)
            VALUES (:loc, :dt, :attr, :val)
        """)
        for _, row in df.iterrows():
            dt = row["tanggal"]
            for col in pollutant_cols:
                if col in df.columns and col in pmap:
                    conn.execute(p_stmt, {"loc": location_id, "dt": dt, "attr": pmap[col], "val": row.get(col)})

def _resolve_pollobs_id(engine: Engine, location_id: int, dt: str, pollutant_code: str) -> int | None:
    # normalize code case
    code = pollutant_code.lower().strip() if pollutant_code else None
    if not code:
        return None
    row = fetch_one(engine, """
        SELECT po.pollobs_id
        FROM pollutant_observation po
        JOIN pollutant_attribute pa ON pa.pollutantattr_id = po.pollutantattr_id
        WHERE po.location_id = :loc AND po.pollobs_date = :dt AND LOWER(pa.pollutantattr_code) = :code
        """, {"loc": location_id, "dt": dt, "code": code})
    return int(row["pollobs_id"]) if row else None

def insert_aqi_daily(engine: Engine, location_id: int, df: pd.DataFrame):
    with engine.begin() as conn:
        for _, row in df.iterrows():
            dt = row["tanggal"]
            kategori = (row.get("kategori_ispu") or "").strip()
            dom = (row.get("polutan_dominan") or "").strip()
            dom_id = _resolve_pollobs_id(engine, location_id, dt, dom)
            # get aqicat_id
            aqicat = fetch_one(engine, "SELECT aqicat_id FROM aqi_category WHERE aqicat_name=:name", {"name": kategori})
            if not aqicat:
                # if category not found, skip row but log
                logger.warning(f"AQI category '{kategori}' not found. Skipping aqidaily for {dt}.")
                continue
            aqicat_id = int(aqicat["aqicat_id"])
            conn.execute(text("""
                INSERT INTO aqi_daily (location_id, aqidaily_date, aqicat_id, dominant_pollobs_id)
                VALUES (:loc, :dt, :aqi, :dom)
                ON DUPLICATE KEY UPDATE aqicat_id=VALUES(aqicat_id), dominant_pollobs_id=VALUES(dominant_pollobs_id)
            """), {"loc": location_id, "dt": dt, "aqi": aqicat_id, "dom": dom_id})
