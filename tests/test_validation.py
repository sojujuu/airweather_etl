import pandas as pd
from etl.validators import infer_city_from_filename

def test_infer_city_from_filename():
    assert infer_city_from_filename("/incoming/cuaca_harian_jakarta.csv") == "jakarta"
    assert infer_city_from_filename("/incoming/ispu_harian_jakarta.csv") == "jakarta"
