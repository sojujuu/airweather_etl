import os, shutil, datetime
import pandas as pd
from sqlalchemy.engine import Engine

from .config import Paths, REQUIRED_WEATHER_COLS, REQUIRED_ISPU_COLS
from .logging_util import get_logger
from .validators import ensure_files_exist, infer_city_from_filename, validate_csv_columns
from .extract import extract_weather, extract_ispu, merge_outer_by_date
from .transform import clean_and_rename
from .db import get_engine
from .load import _get_city_id, _get_location_id_for_city, insert_weather_and_pollutants, insert_aqi_daily

logger = get_logger(__name__)

class AirWeatherPipeline:
    def __init__(self, engine: Engine | None = None):
        self.engine = engine or get_engine()

    def run(self, weather_csv: str, ispu_csv: str):
        # 1) Validate presence
        w_path, i_path = ensure_files_exist(Paths.INCOMING, weather_csv, ispu_csv)

        # 2) Validate columns and separator
        validate_csv_columns(w_path, REQUIRED_WEATHER_COLS, sep=",")
        validate_csv_columns(i_path, REQUIRED_ISPU_COLS, sep=",")

        # 3) Infer city from either filename (require same city token)
        city_w = infer_city_from_filename(w_path)
        city_i = infer_city_from_filename(i_path)
        if city_w.lower() != city_i.lower():
            raise RuntimeError(f"City tokens not aligned: '{city_w}' vs '{city_i}'")
        city_token = city_w

        # 4) Resolve CITY_ID and a default LOCATION_ID
        city_id = _get_city_id(self.engine, city_token)
        location_id = _get_location_id_for_city(self.engine, city_id)
        logger.info(f"Resolved CITY_ID={city_id} LOCATION_ID={location_id} for city '{city_token}'.")

        # 5) Extract
        dfw = extract_weather(w_path)
        dfi = extract_ispu(i_path)

        # 6) Merge
        df_airweather = merge_outer_by_date(dfw, dfi)
        logger.info(f"Merged dataframe shape: {df_airweather.shape}")

        # 7) Transform
        df_clean, bad_rows = clean_and_rename(df_airweather)
        if len(bad_rows):
            logger.warning(f"Dropped {len(bad_rows)} rows with invalid dates.")
        logger.info(f"Clean dataframe shape: {df_clean.shape}")

        # 8) Load
        insert_weather_and_pollutants(self.engine, location_id, df_clean)
        insert_aqi_daily(self.engine, location_id, df_clean)

        # 9) Post-processing (archive/move)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self._archive_file(w_path, f"cuaca_harian_{city_token}_{ts}.csv")
        self._archive_file(i_path, f"ispu_harian_{city_token}_{ts}.csv")
        logger.info("ETL completed successfully.")

    def _archive_file(self, src: str, newname: str):
        dst = os.path.join(Paths.ARCHIVED, newname)
        shutil.move(src, dst)

    def move_failed(self, weather_csv: str, ispu_csv: str):
        # Used by scripts/run_etl.py in exception handling
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        w = os.path.join(Paths.INCOMING, weather_csv)
        i = os.path.join(Paths.INCOMING, ispu_csv)
        if os.path.exists(w):
            shutil.move(w, os.path.join(Paths.FAILED, f"cuaca_harian_{infer_city_from_filename(w)}_{ts}.csv"))
        if os.path.exists(i):
            shutil.move(i, os.path.join(Paths.FAILED, f"ispu_harian_{infer_city_from_filename(i)}_{ts}.csv"))
