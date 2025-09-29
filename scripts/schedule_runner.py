
import argparse, logging, os
from etl.logging_util import get_logger
from datetime import date
from etl.db import get_engine
from sqlalchemy.orm import sessionmaker # type: ignore
from etl.pipeline.pearson_pipeline import PearsonPipeline, month_last_day, last_sunday_before_or_on

def is_last_day_of_month(d: date) -> bool:
    from etl.pipeline.pearson_pipeline import month_last_day as mld
    return d == mld(d)

logger = get_logger('airweather.schedule')

def main():
    parser = argparse.ArgumentParser(description="Automatic scheduler: run weekly and monthly as required.")
    parser.add_argument('--today', type=str, default=None, help="ISO date override, e.g. 2025-10-31")
    args = parser.parse_args()

    today = date.today() if args.today is None else date.fromisoformat(args.today)

    db_url = os.getenv("DATABASE_URL", "mysql+mysqlconnector://root:root@localhost/db_airweather")
    engine = get_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    pipe = PearsonPipeline(session)

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
