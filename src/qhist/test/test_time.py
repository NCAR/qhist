import pytest, datetime
from qhist import qhist

def test_get_period_days():
    output = qhist.get_time_bounds("20250218", "%Y%m%d", "20250310-20250313")
    expected = [datetime.datetime(2025, 3, 10, 0, 0), datetime.datetime(2025, 3, 13, 0, 0)]
    assert output == expected

def test_get_number_days():
    class NewDatetime(datetime.datetime):
        @classmethod
        def today(cls):
            return cls(2025, 3, 1, 0, 0)

    datetime.datetime = NewDatetime
    period = qhist.get_time_bounds("20250218", "%Y%m%d", days = 4)
    output = " ".join([d.strftime("%Y%m%d") for d in period])
    assert output == "20250225 20250301"
