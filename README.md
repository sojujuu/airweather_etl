# 🌤️ AirWeather ETL

AirWeather ETL is a Python-based data pipeline that integrates **daily weather data** (BMKG) with **air quality data** (ISPU Jakarta).  
The system extracts, transforms, and loads (ETL) CSV data into a MySQL star-schema warehouse, and computes **Pearson** and **Spearman correlation** metrics for monitoring the relationship between weather and air pollution.

---

## 🚀 Features

- **ETL Pipeline**  
  - `AirWeatherPipeline`: cleanses and loads weather + ISPU CSVs into MySQL.  
  - `PearsonPipeline`: computes Pearson correlation (with Spearman validation).  

- **Configurable Data Paths** via `etl/config.py`  
- **Logging System** with timestamped log files in `LOG/`  
- **Incoming / Archived Data Management**  
- **Dockerized Deployment** with support for cron-based scheduling  
- **Unit Tests** powered by `pytest`

---

## 📂 Project Structure

```
airweather_etl/
├── INCOMING/               # Raw CSVs to be ingested
│   ├── cuaca_harian_jakarta.csv
│   └── ispu_harian_jakarta.csv
├── ARCHIVED/               # Processed CSVs moved after successful ETL
├── LOG/                    # Log files
├── scripts/
│   ├── run_etl.py          # Main entry to run AirWeather ETL
│   ├── schedule_runner.py  # Runner with cron-like scheduling
│   └── db_ping.py          # Simple DB connectivity check
├── src/etl/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── db.py               # Database utilities
│   ├── config.py           # Centralized config & paths
│   ├── validators.py
│   ├── logging_util.py
│   ├── factories/loader_factory.py
│   ├── strategies/file_loader_strategy.py
│   └── pipeline/
│       ├── airweather_pipeline.py
│       └── pearson_pipeline.py
├── tests/                  # Unit tests
│   ├── test_transform.py
│   ├── test_validation.py
│   └── test_schedule.py
├── requirements.txt
├── pyproject.toml
├── uv.lock
└── README.md
```

---

## ⚙️ Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/airweather_etl.git
   cd airweather_etl
   ```

2. Install dependencies with [uv](https://github.com/astral-sh/uv):
   ```bash
   uv sync
   ```

3. Set up environment variables in `env.txt` or `.env`:
   ```env
   DATABASE_URL=mysql+mysqlconnector://user:password@localhost/db_airweather
   WEATHER_CSV=cuaca_harian_jakarta.csv
   ISPU_CSV=ispu_harian_jakarta.csv
   ```

---

## ▶️ Usage

### Run ETL Manually
```bash
uv run python scripts/run_etl.py
```

### Run Pearson Correlation Pipeline
```bash
uv run python -m etl.pipeline.pearson_pipeline
```

### Check Database Connection
```bash
uv run python scripts/db_ping.py
```

---

## ⏰ Scheduling

This project supports **cron-style scheduling** inside Docker.

Example `docker-compose.yml` service with cron:

```yaml
services:
  etl:
    build: .
    container_name: airweather_etl
    volumes:
      - ./INCOMING:/app/INCOMING
      - ./ARCHIVED:/app/ARCHIVED
      - ./LOG:/app/LOG
    command: ["sh", "-c", "crond -f -l 2"]
```

And define schedules in `etl-cron` format, e.g.:

```cron
# Run ETL every 10 minutes
*/10 * * * * uv run python scripts/run_etl.py

# Run ETL every 1 hour
0 * * * * uv run python scripts/run_etl.py

# Run ETL every 12 hours
0 */12 * * * uv run python scripts/run_etl.py

# Run ETL at specific time (e.g., 01:30)
30 1 * * * uv run python scripts/run_etl.py
```

---

## 🧪 Testing

Run unit tests with:
```bash
uv run pytest
```

---

## 📊 Outputs

- **Database**: MySQL star-schema (`db_airweather`) with dimensions and fact tables  
- **Correlation Results**:  
  - Pearson correlation (`pearson_r`, `pearson_p`)  
  - Spearman validation (`spearman_rho`, `spearman_p`)  
- **Logs**: stored in `LOG/`

---

## 📌 Roadmap

- [ ] Extend ETL to support multi-city weather data  
- [ ] Add real-time streaming option  
- [ ] Build dashboard with Metabase / Grafana  

---

## 📜 License

MIT License
