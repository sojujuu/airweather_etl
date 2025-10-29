import sys, types
# ---- Lightweight stubs to avoid heavy deps ----
sqlalchemy = types.ModuleType("sqlalchemy")
def _text(x): return x
class _Engine: pass
sqlalchemy.text = _text
sqlalchemy.create_engine = lambda *a, **k: _Engine()
sqlalchemy.engine = types.ModuleType("sqlalchemy.engine")
sqlalchemy.engine.Engine = _Engine
sqlalchemy.orm = types.ModuleType("sqlalchemy.orm")
class Session: pass
sqlalchemy.orm.Session = Session
def _sessionmaker(**kw):
    def _factory(*a, **k):
        class _S:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def execute(self, *a, **k):
                class R:
                    def scalar(self): return 1
                    def mappings(self):
                        class M:
                            def first(self): return {}
                            def all(self): return []
                        return M()
                return R()
        return _S()
    return _factory
sqlalchemy.orm.sessionmaker = _sessionmaker
sys.modules['sqlalchemy'] = sqlalchemy
sys.modules['sqlalchemy.engine'] = sqlalchemy.engine
sys.modules['sqlalchemy.orm'] = sqlalchemy.orm

scipy = types.ModuleType("scipy")
stats = types.ModuleType("scipy.stats")
stats.pearsonr = lambda x, y: (0.0, 1.0)
stats.spearmanr = lambda x, y: (0.0, 1.0)
scipy.stats = stats
sys.modules['scipy'] = scipy
sys.modules['scipy.stats'] = stats
# -----------------------------------------------

import pytest
from datetime import date, timedelta
from etl.pipeline.pearson_pipeline import (
    last_sunday_before_or_on,
    month_last_day,
    PearsonPipeline,
    DEFAULT_CITY_ID,  # for asserting city_id param in fetch_pairs
)

# Helper so patched fetch_pairs is compatible with new signature (accepts **kw like city_id)
def fp_rows(rows):
    return lambda s, e, **kw: rows

# Helper to check "is a Sunday <= d and there is no Sunday in (result+1..d)"
def _is_latest_sunday(result: date, d: date) -> bool:
    if not isinstance(result, date):
        return False
    if result > d:
        return False
    if result.weekday() != 6:  # Monday=0..Sunday=6
        return False
    cur = result + timedelta(days=1)
    while cur <= d:
        if cur.weekday() == 6:
            return False
        cur += timedelta(days=1)
    return True

# -----------------------------
# last_sunday_before_or_on
# -----------------------------

@pytest.mark.parametrize("d", [
    date(2024, 9, 1),   # Sunday
    date(2024, 9, 2),   # Monday
    date(2024, 9, 18),  # Wednesday
    date(2024, 2, 29),  # Leap day 2024
    date(2024, 12, 31), # Year end
])
def test_last_sunday_property(d):
    s = last_sunday_before_or_on(d)
    assert _is_latest_sunday(s, d)

# -----------------------------
# month_last_day
# -----------------------------

@pytest.mark.parametrize("d, expected", [
    (date(2024, 1, 10), date(2024, 1, 31)),
    (date(2024, 2, 10), date(2024, 2, 29)),  # leap year
    (date(2023, 2, 10), date(2023, 2, 28)),
    (date(2024, 12, 25), date(2024, 12, 31)),
])
def test_month_last_day(d, expected):
    assert month_last_day(d) == expected

# -----------------------------
# get_date_range_weekly
# -----------------------------

def test_weekly_window_inside_month():
    p = PearsonPipeline()
    # Wed 2024-09-18 -> trailing 7-day window within September: 12..18
    start, end = p.get_date_range_weekly(date(2024, 9, 18))
    assert start == date(2024, 9, 12)
    assert end == date(2024, 9, 18)
    assert start.month == end.month == 9
    # inclusive length must be <= 7
    assert (end - start).days + 1 <= 7

def test_weekly_window_crossing_month_is_clipped_to_month():
    p = PearsonPipeline()
    # Wed 2024-07-31 -> trailing window should be 25..31
    start, end = p.get_date_range_weekly(date(2024, 7, 31))
    assert (start, end) == (date(2024, 7, 25), date(2024, 7, 31))
    assert start.month == 7 and end.month == 7

def test_weekly_window_first_day_of_month():
    p = PearsonPipeline()
    start, end = p.get_date_range_weekly(date(2024, 8, 1))
    assert (start, end) == (date(2024, 8, 1), date(2024, 8, 1))

# -----------------------------
# get_leftover_weekly_range_for_month_end
# -----------------------------

def test_leftover_weekly_range_when_month_ends_midweek():
    p = PearsonPipeline()
    # September 2024 ends on Mon 30 => leftover should be 30..30 (Mon only)
    leftover = p.get_leftover_weekly_range_for_month_end(date(2024, 9, 30))
    assert leftover is not None
    start, end = leftover
    assert start == date(2024, 9, 30)
    assert end == date(2024, 9, 30)

def test_no_leftover_when_month_ends_on_sunday():
    p = PearsonPipeline()
    # May 2020 ended on Sunday 31
    leftover = p.get_leftover_weekly_range_for_month_end(date(2020, 5, 31))
    assert leftover is None

# -----------------------------
# get_date_range_monthly
# -----------------------------

def test_monthly_range_full_month():
    p = PearsonPipeline()
    start, end = p.get_date_range_monthly(date(2024, 2, 15))
    assert start == date(2024, 2, 1)
    assert end == date(2024, 2, 29)
    assert start <= end

# -----------------------------
# Smoke test: pipeline minimal init
# -----------------------------

def test_pipeline_smoke_init():
    p = PearsonPipeline()
    # Should at least have logger bound and methods callable
    assert hasattr(p, "get_date_range_weekly")
    assert callable(p.get_date_range_monthly)

# -----------------------------
# classify coverage
# -----------------------------

@pytest.mark.parametrize("pearson_r, spearman_rho, n_obs, expected", [
    (0.80, 0.78, 100, "STABLE"),
    (-0.75, -0.46, 80, "CONSISTENT_WEAKER"),
    (0.80, 0.30, 100, "NONLINEAR_OR_OUTLIERS"),
    (0.10, 0.60, 100, "NONLINEAR_OR_OUTLIERS"),
    (0.05, 0.04, 5, "INCONCLUSIVE"),
])
def test_classify_branches(pearson_r, spearman_rho, n_obs, expected):
    p = PearsonPipeline()
    assert p.classify(pearson_r, spearman_rho, n_obs) == expected

# -----------------------------
# run_* wrappers delegate to _process_range
# -----------------------------

def test_run_weekly_calls_process_range(monkeypatch):
    p = PearsonPipeline()
    today = date(2024, 7, 31)
    called = {}
    def fake_process(start, end, period_name, processing_date):
        called['args'] = (start, end, period_name, processing_date)
        return 42
    monkeypatch.setattr(p, "_process_range", fake_process)
    ret = p.run_weekly(today)
    assert ret == 42
    s, e, name, d = called['args']
    assert d == today
    assert name == f"WEEK_{s.isoformat()}_{e.isoformat()}"

def test_run_weekly_custom_calls_process_range(monkeypatch):
    p = PearsonPipeline()
    start = date(2024, 9, 30)
    end   = date(2024, 9, 30)
    proc  = date(2024, 9, 30)
    called = {}
    def fake_process(s, e, name, d):
        called['args'] = (s, e, name, d)
        return 7
    monkeypatch.setattr(p, "_process_range", fake_process)
    ret = p.run_weekly_custom(start, end, proc)
    assert ret == 7
    assert called['args'][2] == "WEEK_2024-09-30_2024-09-30"

def test_run_monthly_calls_process_range(monkeypatch):
    p = PearsonPipeline()
    today = date(2024, 2, 15)
    called = {}
    def fake_process(start, end, period_name, processing_date):
        called['args'] = (start, end, period_name, processing_date)
        return 1
    monkeypatch.setattr(p, "_process_range", fake_process)
    ret = p.run_monthly(today)
    assert ret == 1
    s, e, name, d = called['args']
    assert s == date(2024,2,1) and e == date(2024,2,29)
    assert name == "MONTH_202402"
    assert d == today

# -----------------------------
# _process_range coverage (happy path and empty)
# -----------------------------

class FakeDB:
    def __init__(self):
        self.exec_calls = []
        self.commits = 0
    def execute(self, sql, params=None):
        self.exec_calls.append((sql, params))
        class R:
            def fetchall(self): return []
        return R()
    def commit(self):
        self.commits += 1

def test_process_range_happy_path(monkeypatch):
    p = PearsonPipeline()
    # Inject fake DB & data
    fdb = FakeDB()
    p.db = fdb
    # Two groups (corrmet_id)
    rows = [
        # (corrmet_id, obs_date, wx_val, py_val)
        (1, date(2024, 9, 1), 1.0,  2.0),
        (1, date(2024, 9, 2), 2.0,  4.0),
        (2, date(2024, 9, 1), 3.0,  9.0),
        (2, date(2024, 9, 2), 4.0, 16.0),
    ]
    # accept optional city_id without breaking
    monkeypatch.setattr(p, "fetch_pairs", fp_rows(rows))

    # Patch stats to deterministic numbers and classification check
    import etl.pipeline.pearson_pipeline as mod
    monkeypatch.setattr(mod, "pearsonr",  lambda x, y: (0.85, 0.0))
    monkeypatch.setattr(mod, "spearmanr", lambda x, y: (0.83, 0.0))
    # Run
    inserted = p._process_range(
        date(2024,9,1), date(2024,9,2),
        "WEEK_2024-09-01_2024-09-02", date(2024,9,3)
    )
    # Expect 2 inserts, one per corrmet_id
    assert inserted == 2
    assert fdb.commits == 1
    # Verify params of last execute include our classification
    assert any(call[1] is not None and 'desc' in call[1] for call in fdb.exec_calls)

def test_process_range_no_rows(monkeypatch):
    p = PearsonPipeline()
    p.db = FakeDB()
    # fetch_pairs returns empty but still accepts **kw
    monkeypatch.setattr(p, "fetch_pairs", fp_rows([]))
    inserted = p._process_range(
        date(2024,1,1), date(2024,1,1), "WEEK_...", date(2024,1,2)
    )
    assert inserted == 0
    # no commit when nothing inserted
    assert p.db.commits == 0

# -----------------------------
# fetch_pairs shapes the query and returns list (we simulate execute)
# -----------------------------

def test_fetch_pairs_calls_execute_with_bounds(monkeypatch):
    captured = {}
    class DB:
        def execute(self, sql, params):
            captured['params'] = params
            class R:
                def fetchall(self):
                    # New shape: (corrmet_id, obs_date, wx_val, py_val)
                    return [(1, date(2024,1,1), 1.0, 2.0)]
            return R()
    p = PearsonPipeline()
    p.db = DB()
    rows = p.fetch_pairs(date(2024,1,1), date(2024,1,7))
    assert rows and rows[0][0] == 1 and len(rows[0]) == 4
    assert captured['params']['start'] == date(2024,1,1)
    assert captured['params']['end'] == date(2024,1,7)
    # the pipeline passes city_id implicitly (default)
    assert 'city_id' in captured['params']
    assert captured['params']['city_id'] == DEFAULT_CITY_ID
