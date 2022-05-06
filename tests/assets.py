from typing import List, Union

import numpy as np
import pandas as pd
from pandas._testing import assert_frame_equal


def sort_everything(
    df: pd.DataFrame,
    cols_sort: List[str] = None,
) -> pd.DataFrame:
    cols_sort = cols_sort or (
        list(df.columns)
    )
    
    return (
        df
        .sort_values(cols_sort)
        .reset_index(drop=True)
        .sort_index(axis=1)
    )


def assert_frame_equal_wo_order(
    result: pd.DataFrame,
    expected: pd.DataFrame,
    cols_sort: List[str] = None,
    cols_to_str: Union[str, List[str]] = None,
):
    if isinstance(cols_to_str, str):
        assert cols_to_str == "all"
        cols_to_str = list(result.columns)
        _cols_diff = set(cols_to_str) ^ set(expected.columns)
        assert len(_cols_diff) == 0, _cols_diff
        
    result = result.fillna(value=np.nan)
    expected = expected.fillna(value=np.nan)
    
    if cols_to_str is not None:
        for _col in cols_to_str:
            result.loc[:, _col] = result[_col].astype(str)
            expected.loc[:, _col] = expected[_col].astype(str)
    
    result = sort_everything(result, cols_sort)
    expected = sort_everything(expected, cols_sort)
    
    assert_frame_equal(
        result,
        expected,
        rtol=1e-2,
    )
