from typing import Dict
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection
from .db import fetch_scalar, fetch_one
from .logging_util import get_logger

logger = get_logger(__name__)

def _nz(v):
    """Return 0 if value is NaN/NA, else the value as-is."""
    return 0 if pd.isna(v) else v

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

# --- Helpers that can use an existing transaction/connection -----------------

def _get_weatherattr_ids_with_conn(conn: Connection) -> Dict[str, int]:
    rows = conn.execute(text(
        "SELECT weatherattr_id, weatherattr_code FROM weather_attribute"
    )).mappings().all()
    return {r["weatherattr_code"].lower(): int(r["weatherattr_id"]) for r in rows}

def _get_pollutantattr_ids_with_conn(conn: Connection) -> Dict[str, int]:
    rows = conn.execute(text(
        "SELECT pollutantattr_id, pollutantattr_code FROM pollutant_attribute"
    )).mappings().all()
    return {r["pollutantattr_code"].lower(): int(r["pollutantattr_id"]) for r in rows}

def _resolve_pollobs_id_with_conn(conn: Connection, location_id: int, dt: str, pollutant_code: str) -> int | None:
    code = pollutant_code.lower().strip() if pollutant_code else None
    if not code:
        return None
    row = conn.execute(text("""
        SELECT po.pollobs_id
        FROM pollutant_observation po
        JOIN pollutant_attribute pa ON pa.pollutantattr_id = po.pollutantattr_id
        WHERE po.location_id = :loc AND po.pollobs_date = :dt AND LOWER(pa.pollutantattr_code) = :code
    """), {"loc": location_id, "dt": dt, "code": code}).mappings().first()
    return int(row["pollobs_id"]) if row else None

# --- Main loaders (now transaction-aware) -----------------------------------

def insert_weather_and_pollutants(engine: Engine, location_id: int, df: pd.DataFrame, conn: Connection | None = None):
    """
    Insert weather + pollutant observations.
    - If `conn` is provided, uses that connection/transaction.
    - Otherwise, opens its own transaction and commits/rolls back automatically.
    """
    def _do_work(c: Connection):
        wmap = _get_weatherattr_ids_with_conn(c)
        pmap = _get_pollutantattr_ids_with_conn(c)

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

        # WEATHER
        w_stmt = text("""
            INSERT IGNORE INTO weather_observation (location_id, weatherobs_date, weatherattr_id, weatherobs_value)
            VALUES (:loc, :dt, :attr, :val)
        """)
        for _, row in df.iterrows():
            dt = row["tanggal"]
            for code, col in weather_pairs:
                if col in df.columns and code in wmap:
                    c.execute(w_stmt, {"loc": location_id, "dt": dt, "attr": wmap[code], "val": _nz(row.get(col))})

        # POLLUTANTS
        p_stmt = text("""
            INSERT IGNORE INTO pollutant_observation (location_id, pollobs_date, pollutantattr_id, pollobs_value)
            VALUES (:loc, :dt, :attr, :val)
        """)
        for _, row in df.iterrows():
            dt = row["tanggal"]
            for col in pollutant_cols:
                if col in df.columns and col in pmap:
                    c.execute(p_stmt, {"loc": location_id, "dt": dt, "attr": pmap[col], "val": _nz(row.get(col))})

    if conn is not None:
        # Use caller's transaction
        _do_work(conn)
    else:
        # Own transaction scope
        with engine.begin() as c:
            _do_work(c)

def insert_aqi_daily(engine: Engine, location_id: int, df: pd.DataFrame, conn: Connection | None = None):
    """
    Insert/Upsert AQI daily rows.
    - If `conn` is provided, uses that connection/transaction.
    - Otherwise, opens its own transaction and commits/rolls back automatically.
    """
    def _do_work(c: Connection):
        for _, row in df.iterrows():
            dt = row["tanggal"]
            kategori = (row.get("kategori_ispu") or "").strip()
            dom = (row.get("polutan_dominan") or "").strip()
            dom_id = _resolve_pollobs_id_with_conn(c, location_id, dt, dom)

            aqicat = c.execute(
                text("SELECT aqicat_id FROM aqi_category WHERE aqicat_name=:name"),
                {"name": kategori}
            ).mappings().first()
            if not aqicat:
                logger.warning(f"AQI category '{kategori}' not found. Skipping aqidaily for {dt}.")
                continue
            aqicat_id = int(aqicat["aqicat_id"])

            c.execute(text("""
                INSERT INTO aqi_daily (location_id, aqidaily_date, aqicat_id, dominant_pollobs_id)
                VALUES (:loc, :dt, :aqi, :dom)
                ON DUPLICATE KEY UPDATE
                    aqicat_id=VALUES(aqicat_id),
                    dominant_pollobs_id=VALUES(dominant_pollobs_id)
            """), {"loc": location_id, "dt": dt, "aqi": aqicat_id, "dom": dom_id})

    if conn is not None:
        # Use caller's transaction
        _do_work(conn)
    else:
        # Own transaction scope
        with engine.begin() as c:
            _do_work(c)

# --- Convenience wrapper to run BOTH steps atomically -----------------------

def load_all_in_one_transaction(engine: Engine, location_id: int, df: pd.DataFrame):
    """
    Run weather/pollutant inserts and AQI daily inserts in ONE transaction.
    - If any step fails, the whole transaction rolls back.
    - On success, it commits once.
    """
    with engine.begin() as c:
        insert_weather_and_pollutants(engine, location_id, df, conn=c)
        insert_aqi_daily(engine, location_id, df, conn=c)
