import os
from dataclasses import dataclass

from dotenv import load_dotenv
load_dotenv(override=False)

@dataclass(frozen=True)
class Paths:
    BASE_DIR: str = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
    ARCHIVED: str = os.path.join(BASE_DIR, "ARCHIVED")
    FAILED: str = os.path.join(BASE_DIR, "FAILED")
    LOG_DIR: str = os.path.join(BASE_DIR, "LOG")
    INCOMING: str = os.path.join(BASE_DIR, "INCOMING")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    ""
)

# Required columns per spec
REQUIRED_WEATHER_COLS = ["TANGGAL","TN","TX","TAVG","RH_AVG","RR","SS","FF_X","DDD_X","FF_AVG","DDD_CAR"]
REQUIRED_ISPU_COLS    = ["tanggal","stasiun","pm25","pm10","so2","co","o3","no2","max","critical","categori"]

# Canonical rename mapping (lowercase after rename)
RENAME_MAP = {
    "tn":"suhu_min",
    "tx":"suhu_max",
    "tavg":"suhu_avg",
    "rh_avg":"kelembapan_avg",
    "rr":"curah_hujan",
    "ss":"durasi_penyinaran",
    "ff_x":"kecepatan_angin_max",
    "ddd_x":"arah_angin_max",
    "ff_avg":"kecepatan_angin_avg",
    "critical":"polutan_dominan",
    "categori":"kategori_ispu",
}
