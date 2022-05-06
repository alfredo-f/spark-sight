from ast import literal_eval
from pathlib import Path

import pandas as pd

from spark_sight.data_references import (
    COL_TASK_DATE_START,
    COL_TASK_DATE_END,
    COL_STAGE_ASOFTASKS_DATE_START,
    COL_STAGE_ASOFTASKS_DATE_END, COL_SPLIT_BORDERS_LIST,
)
from spark_sight.log_transform.main import (
    determine_borders_of_stages_asoftasks,
    split_on_borders,
)
from tests.assets import assert_frame_equal_wo_order
from tests.shs import ROOT_TESTS_SHS


def _assert_equal_to_expected(
    result,
    expected_file_name: str,
):
    result = result.copy()
    
    expected = pd.read_csv(
        expected_file_name,
        parse_dates=[
            COL_TASK_DATE_START,
            COL_TASK_DATE_END,
            COL_STAGE_ASOFTASKS_DATE_START,
            COL_STAGE_ASOFTASKS_DATE_END,
        ],
        converters={COL_SPLIT_BORDERS_LIST: literal_eval},
    )
    
    for _df in (
        result,
        expected,
    ):
        _df.loc[:, "duration_cpu_usage"] = pd.to_numeric(
            _df["duration_cpu_usage"]
        )
    
    assert_frame_equal_wo_order(
        result,
        expected,
        cols_sort=[
            'id_task', 'id_stage', COL_TASK_DATE_START, COL_TASK_DATE_END
        ]
    )


def test_split_on_borders__split_single_no():
    _input = pd.read_csv(
        Path(ROOT_TESTS_SHS)
        / Path("split_on_borders__split_single_no.csv"),
        parse_dates=[
            COL_TASK_DATE_START,
            COL_TASK_DATE_END,
        ],
    )
    
    borders_all = determine_borders_of_stages_asoftasks(
        _input,
    )
    
    result = split_on_borders(
        _input,
        borders_all,
        metrics=["duration_cpu_usage"],
    )
    
    _assert_equal_to_expected(
        result=result,
        expected_file_name=(
            Path(ROOT_TESTS_SHS)
            / Path("split_on_borders__split_single_no__expected.csv")
        ),
    )
    

def test_split_on_borders__split_multiple():
    _input = pd.read_csv(
        Path(ROOT_TESTS_SHS) / Path("split_on_borders__split_multiple.csv"),
        parse_dates=[
            COL_TASK_DATE_START,
            COL_TASK_DATE_END,
        ]
    )

    borders_all = determine_borders_of_stages_asoftasks(
        _input,
    )
    
    result = split_on_borders(
        _input,
        borders_all,
        metrics=["duration_cpu_usage"],
    )

    _assert_equal_to_expected(
        result=result,
        expected_file_name=(
            Path(ROOT_TESTS_SHS)
            / Path("split_on_borders__split_multiple__expected.csv")
        ),
    )
