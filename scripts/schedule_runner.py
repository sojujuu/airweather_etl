
import argparse, logging, os, traceback
from etl.logging_util import get_logger
from datetime import date
from etl.pipeline.pearson_pipeline import PearsonPipeline
from etl.pipeline.airweather_pipeline import AirWeatherPipeline

def is_last_day_of_month(d: date) -> bool:
    from etl.pipeline.pearson_pipeline import month_last_day as mld
    return d == mld(d)

logger = get_logger('airweather.schedule')

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

    #PearsonPipeline
    parser = argparse.ArgumentParser(description="Automatic scheduler: run weekly and monthly as required.")
    parser.add_argument('--today', type=str, default=None, help="ISO date override, e.g. 2025-10-31")
    args = parser.parse_args()

    today = date.today() if args.today is None else date.fromisoformat(args.today)

    pipe = PearsonPipeline()

    # If Sunday -> weekly
    if today.weekday() == 6:
        logging.info("Today is Sunday -> running weekly window")
        pipe.run_weekly(today)

    # If last day of month -> leftover weekly (if any) then monthly
    if is_last_day_of_month(today):
        logging.info("Today is last day of month -> checking leftover weekly range")
        leftover = pipe.get_leftover_weekly_range_for_month_end(today)
        if leftover:
            start, end = leftover
            logging.info(f"Running leftover weekly range {start}..{end}")
            pipe.run_weekly_custom(start, end, today)
        else:
            logging.info("No leftover weekly range this month-end")
        logging.info("Running monthly window for full month")
        pipe.run_monthly(today)

if __name__ == "__main__":
    main()
