from etl.logging_util import get_logger
from etl.db import get_session
from datetime import date, timedelta
from typing import List, Tuple, Optional
import pandas as pd 
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import text
from scipy.stats import pearsonr, spearmanr

logger = get_logger(__name__)

def month_last_day(d: date) -> date:
    if d.month == 12:
        return date(d.year, 12, 31)
    return date(d.year, d.month + 1, 1) - timedelta(days=1)

def last_sunday_before_or_on(d: date) -> date:
    offset = (d.weekday() - 6) % 7  # Monday=0..Sunday=6
    return d - timedelta(days=offset)

class PearsonPipeline:
    def __init__(self, db_session: Session | None = None):
        self.db = db_session or get_session()

    def get_date_range_weekly(self, today: date) -> Tuple[date, date]:
        start = today - timedelta(days=6)
        if start.month != today.month:
            start = date(today.year, today.month, 1)
        return (start, today)

    def get_date_range_monthly(self, today: date) -> Tuple[date, date]:
        start = date(today.year, today.month, 1)
        end = month_last_day(today)
        return (start, end)

    def get_leftover_weekly_range_for_month_end(self, today: date) -> Optional[Tuple[date, date]]:
        end = month_last_day(today)
        last_sun = last_sunday_before_or_on(end)
        start = last_sun + timedelta(days=1)
        if start.month == end.month and start <= end:
            return (start, end)
        return None

    def fetch_pairs(self, start: date, end: date) -> List[Tuple]:
        sql = text(
            """
            SELECT cm.corrmet_id, wo.location_id,
                    wo.weatherobs_date, wo.weatherobs_value, po.pollobs_value
            FROM correlation_metrics cm
            JOIN weather_observation wo ON wo.weatherattr_id = cm.weather_x
            JOIN pollutant_observation po 
                    ON po.pollutantattr_id = cm.pollutant_y
                AND po.pollobs_date = wo.weatherobs_date
                AND po.location_id = wo.location_id
            WHERE cm.is_active = 1
                AND wo.weatherobs_date BETWEEN :start AND :end
            ORDER BY cm.corrmet_id, wo.location_id, wo.weatherobs_date
            """
        )
        rows = self.db.execute(sql, {"start": start, "end": end}).fetchall()
        return rows

    def classify(self, pearson_r, spearman_rho, n_obs):
        if np.sign(pearson_r) != np.sign(spearman_rho):
            return "UNRELIABLE"
        if max(abs(pearson_r), abs(spearman_rho)) < 0.20 or n_obs < 12:
            return "INCONCLUSIVE"
        if abs(pearson_r - spearman_rho) < 0.20 and min(abs(pearson_r), abs(spearman_rho)) >= 0.30:
            return "STABLE"
        if abs(pearson_r - spearman_rho) < 0.40:
            return "CONSISTENT_WEAKER"
        return "NONLINEAR_OR_OUTLIERS"

    def _process_range(self, start: date, end: date, period_name: str, processing_date: date):
        logger.info(f"Processing range {start} to {end} as {period_name}")
        rows = self.fetch_pairs(start, end)
        if not rows:
            logger.warning("No rows found for period %s", period_name)
            return 0

        df = pd.DataFrame(rows, columns=["corrmet_id", "location_id", "obs_date", "wx_val", "py_val"])
        inserted = 0
        for (corrmet_id, location_id), g in df.groupby(["corrmet_id", "location_id"]):
            wx = g["wx_val"].astype(float).values
            py = g["py_val"].astype(float).values
            if len(wx) < 2 or len(py) < 2:
                continue
            try:
                pearson_r, _ = pearsonr(wx, py)
                spearman_rho, _ = spearmanr(wx, py)
            except Exception as e:
                logger.error("Correlation error for corrmet_id=%s loc=%s: %s", corrmet_id, location_id, e)
                continue

            classification = self.classify(pearson_r, spearman_rho, len(wx))
            insert_sql = text(
                """
                INSERT INTO correlation_result
                (location_id, corrmet_id, period_name, processing_date, val_result, n_samples)
                SELECT :loc, :corr, :period, :proc, cf.corrflag_id, :n
                FROM correlation_flag cf WHERE cf.corrflag_desc = :desc
                """
            )
            self.db.execute(
                insert_sql,
                {
                    "loc": location_id,
                    "corr": corrmet_id,
                    "period": period_name,
                    "proc": processing_date,
                    "n": int(len(wx)),
                    "desc": classification,
                },
            )
            inserted += 1
        self.db.commit()
        logger.info("Inserted %s correlation_result rows for %s", inserted, period_name)
        return inserted

    def run_weekly(self, today: date) -> int:
        start, end = self.get_date_range_weekly(today)
        period_name = f"WEEK_{start.isoformat()}_{end.isoformat()}"
        return self._process_range(start, end, period_name, today)

    def run_weekly_custom(self, start: date, end: date, processing_date: date) -> int:
        period_name = f"WEEK_{start.isoformat()}_{end.isoformat()}"
        return self._process_range(start, end, period_name, processing_date)

    def run_monthly(self, today: date) -> int:
        start, end = self.get_date_range_monthly(today)
        period_name = f"MONTH_{start.strftime('%Y%m')}"
        return self._process_range(start, end, period_name, today)
