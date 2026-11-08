from datetime import datetime
from src.batch import parse_line
GOOD='16/12/2006;17:24:00;4.216;0.418;234.840;18.400;0.000;1.000;17.000'
def test_units_and_order():
    r=parse_line(GOOD)
    assert r[0]==datetime(2006,12,16,17,24) and r[1]==4.216 and r[-1]==17
    assert abs(r[1]/60-0.07026666666666667)<1e-12
def test_missing_malformed_nonfinite_and_negative():
    for value in ['?', '', 'NaN','inf','-1']:
        assert parse_line(GOOD.replace('4.216',value)) is None
    assert parse_line('Date;Time;Global_active_power') is None
    assert parse_line(GOOD.replace('16/12/2006','32/12/2006')) is None
    assert parse_line(GOOD+';0') is None

def test_zero_power_is_valid():
    assert parse_line(GOOD.replace('4.216','0')) is not None

def test_source_wall_clock_serialization_is_timezone_stable():
    # Regression: an Asia/Kolkata host must not move source rows across months.
    assert datetime(1970,1,1).timestamp()==0
    r=parse_line(GOOD.replace('16/12/2006;17:24:00','01/01/2010;00:00:00'))
    assert r[0].timestamp()==1262304000
