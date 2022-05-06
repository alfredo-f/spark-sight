from pathlib import Path

import numpy as np
import pandas as pd

from spark_sight.data_references import (
    COL_TASK_DATE_START,
    COL_TASK_DATE_END,
)
from spark_sight.log_transform.main import \
    (
    determine_borders_of_stages_asoftasks,
)
from tests.log_transform import ROOT_TESTS_LOG_TRANSFORM


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
