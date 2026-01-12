import pandas as pd
from src.cleaning import remove_duplicates, handle_missing_values, convert_types, filter_valid_rows

def test_remove_duplicates():
    df = pd.DataFrame({"id": [1,1,2]})
    out = remove_duplicates(df)
    assert len(out) == 2

def test_handle_missing_values():
    df = pd.DataFrame({"age": [None], "salary": [None]})
    out = handle_missing_values(df)
    assert out.loc[0, "age"] == 0
    assert out.loc[0, "salary"] == 0

def test_convert_types():
    df = pd.DataFrame({"age": ["20"]})
    out = convert_types(df)
    assert out["age"].dtype == int

def test_filter_valid_rows():
    df = pd.DataFrame({"salary": [0, 100, -10]})
    out = filter_valid_rows(df)
    assert len(out) == 1
