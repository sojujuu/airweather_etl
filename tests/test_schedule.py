
from datetime import date
from etl.pipeline.pearson_pipeline import last_sunday_before_or_on, PearsonPipeline

class DummySession:
    def execute(self, *a, **k):
        class R:
            def fetchall(self): return []
        return R()
    def commit(self): pass

def test_last_sunday_calculation():
    d = date(2025,10,31)  # Friday
    assert last_sunday_before_or_on(d).isoformat() == '2025-10-26'

def test_leftover_range_oct_2025():
    p = PearsonPipeline(DummySession())
    d = date(2025,10,31)
    leftover = p.get_leftover_weekly_range_for_month_end(d)
    assert leftover == (date(2025,10,27), date(2025,10,31))

def test_weekly_window_same_month_guard():
    p = PearsonPipeline(DummySession())
    start, end = p.get_date_range_weekly(date(2024,9,1))  # Sun
    assert (start, end) == (date(2024,9,1), date(2024,9,1))
