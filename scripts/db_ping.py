import os, sys, traceback
from sqlalchemy import create_engine, text # type: ignore
from sqlalchemy.engine import make_url   # type: ignore

from dotenv import load_dotenv
load_dotenv(override=False)

url_str = os.getenv(
    "DATABASE_URL",
    ""
)

# Show which URL will actually be used (without password)
try:
    url = make_url(url_str)
    print("Using DATABASE_URL:", url.render_as_string(hide_password=True))
except Exception:
    print("Using DATABASE_URL (raw):", url_str)

# Build engine and attempt a simple ping
engine = create_engine(url_str, pool_pre_ping=True, connect_args={"connect_timeout": 5})

try:
    with engine.connect() as conn:
        print("SELECT 1 →", conn.execute(text("SELECT 1")).scalar())
        print("VERSION() →", conn.execute(text("SELECT VERSION()")).scalar())
        print("CURRENT_USER() →", conn.execute(text("SELECT CURRENT_USER()")).scalar())
        print("DATABASE() →", conn.execute(text("SELECT DATABASE()")).scalar())
        print("✅ Connectivity OK")
except Exception as e:
    print("❌ Connectivity FAILED:", type(e).__name__, e)
    traceback.print_exc()
    sys.exit(1)
