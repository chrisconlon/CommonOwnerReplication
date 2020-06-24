import pandas as pd

def lookup_dates(s):
    """
    This is an extremely fast approach to datetime parsing.
    For large data, the same dates are often repeated. Rather than
    re-parse these, we store all unique dates, parse them, and
    use a lookup to convert all dates.
    """
    dates_dict = {date:pd.to_datetime(date,errors='coerce') for date in s.unique()}
    return s.map(dates_dict)

def end_quarter(series):
    return (series - pd.tseries.offsets.DateOffset(days=1) + pd.tseries.offsets.QuarterEnd())