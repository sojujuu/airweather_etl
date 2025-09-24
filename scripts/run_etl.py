import sys, os, traceback
from etl.pipeline import AirWeatherPipeline
from etl.logging_util import get_logger
from etl.config import Paths

logger = get_logger("airweather.run")

def main():
    # default filenames for Jakarta
    weather_csv = os.environ.get("WEATHER_CSV", "cuaca_harian_jakarta.csv")
    ispu_csv    = os.environ.get("ISPU_CSV", "ispu_harian_jakarta.csv")

    pipeline = AirWeatherPipeline()
    try:
        pipeline.run(weather_csv, ispu_csv)
    except Exception as e:
        logger.error(f"ETL FAILED: {e}")
        traceback.print_exc()
        pipeline.move_failed(weather_csv, ispu_csv)
        sys.exit(1)

if __name__ == "__main__":
    main()
