import re
from pathlib import Path

import re
from pathlib import Path

import numpy as np
import pandas as pd
from pandas import CategoricalDtype

from spark_sight.data_references import (
    COL_TASK_DATE_START,
    COL_TASK_DATE_END, COL_SUBSTAGE_DATE_INTERVAL,
)
from spark_sight.log_transform.main import \
    (
    determine_borders_of_stages_asoftasks, check_tasks_are_split_correctly,
)
from tests.log_transform import ROOT_TESTS_LOG_TRANSFORM


def interval_type(s):
    """Parse interval string to Interval"""
    
    regex = r"\(([0-9\-:\. ]+), ([0-9\-:\. ]+)\]"
    _match = re.match(regex, s)
    if _match is None:
        raise ValueError(f"Does not match regex: {s}")
    _groups = _match.groups()
    if len(_groups) != 2:
        raise ValueError(f"Didn't find two groups: {_groups}")
    
    left, right = _groups
    left = pd.to_datetime(left)
    right = pd.to_datetime(right)
    
    left_closed = s.startswith('[')
    right_closed = s.endswith(']')
    
    closed = 'neither'
    if left_closed and right_closed:
        closed = 'both'
    elif left_closed:
        closed = 'left'
    elif right_closed:
        closed = 'right'
    
    return pd.Interval(left, right, closed=closed)


def test_determine_borders_of_stages_asoftasks():
    
    _input = pd.read_csv(
        Path(ROOT_TESTS_LOG_TRANSFORM)
        / Path("determine_borders_of_stages_asoftasks__input.csv"),
        parse_dates=[
            COL_TASK_DATE_START,
            COL_TASK_DATE_END,
        ]
    )
    
    result = determine_borders_of_stages_asoftasks(
        _input
    )
    
    assert result == [
        np.datetime64('2022-03-04T12:56:59.999999999'),
        np.datetime64('2022-03-04T12:58:00.000000000'),
        np.datetime64('2022-03-04T13:00:00.000000000'),
        np.datetime64('2022-03-04T13:01:00.000000000'),
        np.datetime64('2022-03-04T13:02:00.000000000'),
        np.datetime64('2022-03-04T13:30:00.000000000'),
        np.datetime64('2022-03-04T13:31:00.000000000'),
        np.datetime64('2022-03-04T13:32:00.000000000'),
    ]


def test_check_tasks_are_split_correctly():
    _input = pd.read_csv(
        Path(ROOT_TESTS_LOG_TRANSFORM)
        / Path("check_tasks_are_split_correctly.csv"),
        parse_dates=[
            COL_TASK_DATE_START,
            COL_TASK_DATE_END,
        ],
    )
    
    _input.loc[:, COL_SUBSTAGE_DATE_INTERVAL] = (
        _input[COL_SUBSTAGE_DATE_INTERVAL]
        .apply(interval_type)
    )

    _input.loc[:, COL_SUBSTAGE_DATE_INTERVAL] = (
        _input[COL_SUBSTAGE_DATE_INTERVAL]
        .astype(
            CategoricalDtype(
                categories=(
                    sorted(
                        _input[COL_SUBSTAGE_DATE_INTERVAL].unique()
                    )
                ),
                ordered=True,
            )
        )
    )
    
    check_tasks_are_split_correctly(
        _input
    )
