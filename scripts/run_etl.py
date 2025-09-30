import sys, os, traceback, argparse
from datetime import date
from etl.pipeline.airweather_pipeline import AirWeatherPipeline
from etl.pipeline.pearson_pipeline import PearsonPipeline
from etl.logging_util import get_logger

logger = get_logger("airweather.run")


def main():
    #AirWeatherPipeline
    weather_csv = os.environ.get("WEATHER_CSV", "cuaca_harian_jakarta.csv")
    ispu_csv    = os.environ.get("ISPU_CSV", "ispu_harian_jakarta.csv")

    pipeline = AirWeatherPipeline()
    try:
        pipeline.run(weather_csv, ispu_csv)
    except Exception as e:
        logger.error(f"ETL FAILED: {e}")
        traceback.print_exc()
        pipeline.move_failed(weather_csv, ispu_csv)

    #PeasonPipeline
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['weekly','monthly'], required=True)
    parser.add_argument('--today', type=str, default=None)
    args = parser.parse_args()

    today = date.today() if args.today is None else date.fromisoformat(args.today)

    pipeline = PearsonPipeline()
    if args.mode == 'weekly':
        pipeline.run_weekly(today)
    else:
        pipeline.run_monthly(today)

if __name__ == "__main__":
    main()
