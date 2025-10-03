FROM python:3.12-slim

# 1) System deps (cron + SSL + tzdata). wheel helps for native deps.
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron ca-certificates tzdata gcc build-essential \
 && rm -rf /var/lib/apt/lists/*

# 2) Non-root user for better security
ARG APP_USER=appuser
ARG APP_UID=1000
RUN useradd -m -u ${APP_UID} -s /bin/bash ${APP_USER}

# 3) Install uv (package manager)
#    https://github.com/astral-sh/uv — single binary installer
RUN pip install --no-cache-dir uv

WORKDIR /app

# 4) Copy only resolution files first for better layer caching
COPY pyproject.toml uv.lock ./

# 5) Sync deps (respect lockfile)
#    --frozen => must match lock; --no-dev => skip dev deps if you used that group
RUN uv sync --frozen --no-dev --no-install-project

# 6) Copy source code
#    (zip Anda: airweather_etl-main.zip → pastikan ter-extract ke struktur /app)
COPY . .

# 7) Now install the project itself (editable or normal as per pyproject)
RUN uv sync --frozen --no-dev

# 8) Ensure data dirs exist (these will be bind-mount ke host via compose)
RUN mkdir -p /data/ARCHIVED /data/FAILED /data/INCOMING /data/LOG \
 && chown -R ${APP_USER}:${APP_USER} /data /app

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_SYSTEM_PYTHON=1 \
    TZ=Asia/Jakarta

USER ${APP_USER}

# 9) Default command = help
#    Service "scheduler" akan override command untuk menjalankan cron
CMD ["bash", "-lc", "echo 'AirWeather image ready. Use docker compose services.' && sleep infinity"]
