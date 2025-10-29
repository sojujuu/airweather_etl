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

# --- City / Location constants for city-level aggregation ---
DEFAULT_CITY_ID = 1          # Jakarta
CITY_AGG_LOC_ID = 6          # location_id = 'CITY_AGG_JKT' (CITY_AGG_JAKARTA)

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

    def fetch_pairs(self, start: date, end: date, city_id: int = DEFAULT_CITY_ID) -> List[Tuple]:
        """
        Ambil pasangan nilai WX dan PY yang sudah di-AGGREGATE per HARI pada level kota.

        Perbedaan dibanding versi lama (per stasiun):
        - Kita bentuk deret tanggal `d` dari UNION weather_observation & pollutant_observation,
        sehingga semua hari yang ada di salah satu sisi tetap muncul (kalender harian utuh).
        - weather_observation (wo) dan pollutant_observation (po) tetap di-LEFT JOIN ke `d`,
        agar hari tidak hilang saat salah satu sisi kosong.
        - Pembatasan kota DIPINDAH ke dalam agregasi:
            AVG(CASE WHEN lw.city_id = :city_id THEN wo.weatherobs_value END) AS wx_val
            AVG(CASE WHEN lp.city_id = :city_id THEN po.pollobs_value    END) AS py_val
        → hanya nilai dari kota yang dimaksud yang dihitung, tanpa menghapus harinya.
        - Hasil akhirnya: 1 baris per (corrmet_id, tanggal) dengan wx_val & py_val
        berupa rata-rata harian seluruh stasiun di kota tersebut (city-average).
        - Dengan granularitas harian yang konsisten, weekly biasanya ~7 titik dan monthly ~28–31,
        sehingga risiko INCONCLUSIVE karena n terlalu kecil jauh berkurang.
        """
        sql = text(
            """
            SELECT
            cm.corrmet_id,
            d.dt AS obs_date,
            AVG(CASE WHEN lw.city_id = :city_id THEN wo.weatherobs_value END) AS wx_val,
            AVG(CASE WHEN lp.city_id = :city_id THEN po.pollobs_value    END) AS py_val
            FROM correlation_metrics cm

            -- Deret tanggal: gabungan tanggal yang ada di salah satu sisi (cuaca/polutan)
            JOIN (
            SELECT weatherobs_date AS dt
            FROM weather_observation
            WHERE weatherobs_date BETWEEN :start AND :end
            UNION
            SELECT pollobs_date AS dt
            FROM pollutant_observation
            WHERE pollobs_date BETWEEN :start AND :end
            ) d

            -- Sisi cuaca: LEFT JOIN agar hari tetap muncul jika tidak ada data cuaca
            LEFT JOIN weather_observation wo
                ON wo.weatherattr_id   = cm.weather_x
                AND wo.weatherobs_date  = d.dt
            LEFT JOIN location lw
                ON lw.location_id      = wo.location_id

            -- Sisi polutan: LEFT JOIN agar hari tetap muncul jika tidak ada data polutan
            LEFT JOIN pollutant_observation po
                ON po.pollutantattr_id = cm.pollutant_y
                AND po.pollobs_date     = d.dt
            LEFT JOIN location lp
                ON lp.location_id      = po.location_id

            WHERE cm.is_active = 1
            GROUP BY cm.corrmet_id, d.dt
            ORDER BY cm.corrmet_id, d.dt
            """
        )
        rows = self.db.execute(
            sql, {"start": start, "end": end, "city_id": city_id}
        ).fetchall()
        return rows

    # Versi lama, sebelum pakai city-level aggregatio
    # def fetch_pairs(self, start: date, end: date) -> List[Tuple]:
    #     sql = text(
    #         """
    #         SELECT cm.corrmet_id, wo.location_id,
    #                 wo.weatherobs_date, wo.weatherobs_value, po.pollobs_value
    #         FROM correlation_metrics cm
    #         JOIN weather_observation wo ON wo.weatherattr_id = cm.weather_x
    #         JOIN pollutant_observation po 
    #                 ON po.pollutantattr_id = cm.pollutant_y
    #             AND po.pollobs_date = wo.weatherobs_date
    #             AND po.location_id = wo.location_id
    #         WHERE cm.is_active = 1
    #             AND wo.weatherobs_date BETWEEN :start AND :end
    #         ORDER BY cm.corrmet_id, wo.location_id, wo.weatherobs_date
    #         """
    #     )
    #     rows = self.db.execute(sql, {"start": start, "end": end}).fetchall()
    #     return rows

    # Tambahkan helper kecil di atas/sekitar fungsi classify
    @staticmethod
    def _min_n_for_period(period_name: str) -> int:
        # Weekly window biasanya 5–7 observasi efektif
        if period_name.upper().startswith("WEEK"):
            return 5
        return 12

    def classify(self, pearson_r: float, spearman_rho: float, n_obs: int, period_name: str | None = None, p_p: float | None = None,p_s: float | None = None, alpha: float | None = None) -> str:

        """
        Versi baru tetap kompatibel:
        - Jika period_name=None dan p_p/p_s=None => fallback ke logika lama.
        - Jika period_name ada => pakai ambang n dinamis.
        - Jika p_p & p_s ada => pakai signifikansi.
        """

        # 0) Tentukan alpha efektif
        if alpha is None:
            per = (period_name or "").upper()
            if per.startswith("WEEK"):
                eff_alpha = 0.20
            else:
                eff_alpha = 0.10
        else:
            eff_alpha = alpha

        # 1) Ambang jumlah sampel
        if period_name is None:
            # fallback lama: weekly pun bakal sering INCONCLUSIVE
            if n_obs < 12:
                return "INCONCLUSIVE"
        else:
            if n_obs < self._min_n_for_period(period_name):
                return "INCONCLUSIVE"

        # 2) Tanda berlawanan => tidak reliabel
        if np.sign(pearson_r) != np.sign(spearman_rho):
            return "UNRELIABLE"

        # 3) Jika p-value tersedia: keduanya tak signifikan => INCONCLUSIVE
        if (p_p is not None and p_s is not None) and (p_p >= eff_alpha and p_s >= eff_alpha):
            return "INCONCLUSIVE"

        # 4) Kekuatan dan kedekatan nilai
        delta = abs(pearson_r - spearman_rho)
        min_abs = min(abs(pearson_r), abs(spearman_rho))
        
        if delta < 0.20 and min_abs >= 0.30:
            return "STABLE"
        if delta < 0.40:
            return "CONSISTENT_WEAKER"
        return "NONLINEAR_OR_OUTLIERS"

    def _process_range(self, start: date, end: date, period_name: str, processing_date: date):
        logger.info(f"Processing range {start} to {end} as {period_name}")

        # 2a) Ambil data AGG kota per hari
        rows = self.fetch_pairs(start, end, city_id=DEFAULT_CITY_ID)
        if not rows:
            logger.warning("No rows found for period %s", period_name)
            return 0
        
        # 2b) DF TANPA location_id; group by corrmet_id saja
        df = pd.DataFrame(rows, columns=["corrmet_id", "obs_date", "wx_val", "py_val"])
        inserted = 0

        for corrmet_id, g in df.groupby("corrmet_id"):
            wx = g["wx_val"].astype(float).values
            py = g["py_val"].astype(float).values

            # 1. Bersihkan data dari NaN / inf
            mask = np.isfinite(wx) & np.isfinite(py)
            wx, py = wx[mask], py[mask]

            # 2. Cek minimal panjang
            if len(wx) < 2 or len(py) < 2:
                continue

            # 3. Kalau varians nol → semua nilai sama → korelasi meaningless
            if np.nanstd(wx) == 0 or np.nanstd(py) == 0:
                classification = "INCONCLUSIVE"
            else:
                try:
                    pearson_r, p_p = pearsonr(wx, py)
                    spearman_rho, p_s = spearmanr(wx, py)
                except Exception as e:
                    logger.error("Correlation error for corrmet_id=%s: %s", corrmet_id, e)
                    continue

                # 4. Panggil classify baru dengan parameter tambahan
                classification = self.classify(
                    pearson_r=pearson_r,
                    spearman_rho=spearman_rho,
                    n_obs=len(wx),
                    period_name=period_name,  # auto: WEEK* => 0.20, selain itu => 0.10
                    p_p=p_p,                  # p-value Pearson
                    p_s=p_s,                  # p-value Spearman
                    # alpha=0.10,               # default threshold signifikansi
                )

            # 5. Simpan hasil ke tabel correlation_result (INSERT: gunakan location_id agregat kota)
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
                    "loc": CITY_AGG_LOC_ID, # pakai lokasi agregat kota
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
