# AirWeather ETL (Jakarta)
Object‑oriented ETL pipeline (Factory + Strategy style) for merging Jakarta daily weather and ISPU air‑quality CSVs, cleansing, and loading into the `db_airweather` MySQL data warehouse.

## Requirements you provided
- Data feeds: `cuaca_harian_*.csv`, `ispu_harian_*.csv`
- Database schema: `db_airweather.sql`
- Static master data: `static_data.sql`
- Sample loader program (pattern reference): `loaders.zip` (this solution follows Factory + Strategy)

## What this project does
1. **Validate files** under `INCOMING/`:
   - Both CSVs must exist and be comma‑separated.
   - File names must end with the city before `.csv` (e.g., `..._jakarta.csv`), which must exist in `city.name`. The resolved `location_id` is looked up from `location` table using the city's id (first match), and `CITY_ID` is carried across loads.
   - Required columns are verified (see below). Column `DDD_CAR` is ignored if present.
2. **Extract & Merge** with outer join by `tanggal` to form `df_airweather` (ascending by date).
3. **Transform** (missing‑value ffill/bfill, date normalization `YYYY-MM-DD`, drop bad rows, rename to snake_case per spec).
4. **Load** into MySQL tables:
   - `weather_observation`
   - `pollutant_observation`
   - `aqi_daily` (including `dominant_pollobs_id` resolution)
5. **Post‑processing**:
   - On success: archive files into `ARCHIVED/` with timestamp.
   - On failure: move files into `FAILED/` with timestamp.
6. **Logging**:
   - Detailed processing logs are written into `LOG/LOG_YYYYMMDD_HHMMSS.log`.
7. **Unit tests**:
   - Core validation and transform functions are covered. DB tests are optional and skipped unless `AIRWEATHER_ENABLE_DB_TESTS=1`.

## Project layout
```
airweather_etl/
  INCOMING/           # put your two CSVs here
  ARCHIVED/
  FAILED/
  LOG/
  etl/
    __init__.py
    config.py
    logging_util.py
    db.py
    validators.py
    extract.py
    transform.py
    load.py
    pipeline.py
    strategies/
      __init__.py
      file_loader_strategy.py
    factories/
      __init__.py
      loader_factory.py
  tests/
    test_validation.py
    test_transform.py
  scripts/
    run_etl.py
  README.md
  requirements.txt
  .env.example
```

## How to run
1. Ensure MySQL is running and execute your `db_airweather.sql` and `static_data.sql` first.
2. Put your CSV files into `INCOMING/`, for example:
   - `INCOMING/cuaca_harian_jakarta.csv`
   - `INCOMING/ispu_harian_jakarta.csv`
3. Configure database URL in environment. Create `.env` from `.env.example` or set OS env var.
4. Install dependencies:
   ```bash
   brew install astral-sh/uv/uv
   uv venv 
   uv pip install -r requirements.txt
   uv run pytest
   ```
5. Run the ETL:
   ```bash
   uv run python scripts/run_etl.py
   ```

## Required columns
- **Weather** `cuaca_harian_*.csv`: `TANGGAL,TN,TX,TAVG,RH_AVG,RR,SS,FF_X,DDD_X,FF_AVG,DDD_CAR` (note: `DDD_CAR` will be ignored if present)
- **ISPU** `ispu_harian_*.csv`: `Tanggal,stasiun,pm25,pm10,so2,co,o3,no2,max,critical,categori`

## Notes
- `pollutant_attribute.pollutantattr_code` may be uppercase in your master data. Loader normalizes comparisons case‑insensitively, so `pm10` and `PM10` match.
- `dominant_pollobs_id` is set by finding the matching `pollutant_observation` row for the day and dominant pollutant code.
- All inserts are batched within a single transaction. On any error the transaction is rolled back and files are moved to `FAILED/`.


## How to run scheduler
   ```bash
   uv run python scripts/schedule_runner.py
   uv run python scripts/schedule_runner.py --today 2025-10-31
   ```