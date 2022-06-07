import json
import logging
from datetime import datetime
from json import JSONDecodeError
from pathlib import Path

import pandas as pd
import pytest

from spark_sight.execute import _main
from spark_sight.util import configure_pandas
from tests import ROOT_TESTS


APP_LONG_APPLICATION_NAME = "test_e2e.txt"
APP_LONG_DPU = 5
APP_LONG_DPU_CPU = 8  # G.2X
APP_LONG_FILE_PATH = (
    Path(ROOT_TESTS)
    / Path("test_e2e_spill_true")
)


APP_LONG_CLUSTER_CPU_NUMBER_TOTAL: int = (
    (
        # One of the DPU is the Master Node
        (APP_LONG_DPU - 1)
        * APP_LONG_DPU_CPU
    )
    # Driver
    - 1
)

APP_LONG_FILE = open(
    APP_LONG_FILE_PATH,
    "r",
)

APP_LONG_LINES_UNPARSED = APP_LONG_FILE.readlines()[:1200]
APP_LONG_LINES = []

for _ in (
    APP_LONG_LINES_UNPARSED
):
    try:
        APP_LONG_LINES.append(
            json.loads(_)
        )
        
    except JSONDecodeError as e:
        logging.debug(f"Unterminated line : {_}")


STAGE_IDS = set(
    _["Stage Info"]["Stage ID"]
    for _ in APP_LONG_LINES
    if _["Event"] == "SparkListenerStageCompleted"
)


@pytest.mark.parametrize(
    "spill_bool",
    [
        True,
        False,
    ]
)
def test_e2e(
    spill_bool: bool,
):
    configure_pandas(
        pd,
    )

    path_spark_event_log = (
        f"test_e2e_spill_{str(spill_bool).lower()}"
    )
    
    fig = _main(
        path_spark_event_log=(
            Path(ROOT_TESTS)
            / Path(path_spark_event_log)
        ),
        cpus=32,
    )
    
    fig.show()
    
    INDEX_DATA_CHART_EFFICIENCY_WORK = 0
    INDEX_DATA_CHART_EFFICIENCY_SER = 1
    INDEX_DATA_CHART_EFFICIENCY_SHUFFLE = 2
    INDEX_DATA_CHART_SPILL = 3

    assert (
        fig.data[INDEX_DATA_CHART_EFFICIENCY_WORK].hovertext[9]
        == '13:00:07 to 13:00:20 (13 sec)<br><b>66.6%</b> (Actual task work)<br>Stage: <b>21</b>'
    )
    
    assert (
        fig.data[INDEX_DATA_CHART_EFFICIENCY_WORK].x[15]
        == datetime(2022, 3, 30, 13, 0, 56, 387999)
    )
    assert (
        fig.data[INDEX_DATA_CHART_EFFICIENCY_WORK].y[15]
        == 0.8616790426854759
    )
    assert (
        fig.data[INDEX_DATA_CHART_EFFICIENCY_WORK].marker.color
        == 'green'
    )
    assert (
        fig.data[INDEX_DATA_CHART_EFFICIENCY_WORK].hovertext[15]
        == '13:00:56 to 13:01:09 (13 sec)<br><b>86.2%</b> (Actual task work)<br>Stages: <b>36, 37</b>'
    )
    
    assert (
        fig.data[INDEX_DATA_CHART_EFFICIENCY_SER].x[15]
        == datetime(2022, 3, 30, 13, 0, 56, 387999)
    )
    assert (
        fig.data[INDEX_DATA_CHART_EFFICIENCY_SER].y[15]
        == 0.0004172841605397031
    )
    assert (
        fig.data[INDEX_DATA_CHART_EFFICIENCY_SER].marker.color
        == 'violet'
    )
    assert (
        fig.data[INDEX_DATA_CHART_EFFICIENCY_SER].hovertext[15]
        == '13:00:56 to 13:01:09 (13 sec)<br><b>0.0%</b> (Serialization, deserialization)<br>Stages: <b>36, 37</b>'
    )
    
    assert (
        fig.data[INDEX_DATA_CHART_EFFICIENCY_SHUFFLE].x[15]
        == datetime(2022, 3, 30, 13, 0, 56, 387999)
    )
    assert (
        fig.data[INDEX_DATA_CHART_EFFICIENCY_SHUFFLE].y[15]
        == 0.026342815821321402
    )
    assert (
        fig.data[INDEX_DATA_CHART_EFFICIENCY_SHUFFLE].marker.color
        == 'red'
    )
    assert (
        fig.data[INDEX_DATA_CHART_EFFICIENCY_SHUFFLE].hovertext[15]
        == '13:00:56 to 13:01:09 (13 sec)<br><b>2.6%</b> (Shuffle read and write)<br>Stages: <b>36, 37</b>'
    )
    
    if spill_bool:
        assert (
            fig.data[INDEX_DATA_CHART_SPILL].x[0]
            == 21200.0
        )
        assert (
            fig.data[INDEX_DATA_CHART_SPILL].x[2]
            == 12520.0
        )
        assert (
            fig.data[INDEX_DATA_CHART_SPILL].marker.color[1]
            == 1590212564.0
        )
        assert (
            fig.data[INDEX_DATA_CHART_SPILL].marker.color[3]
            == 900212564.0
        )
        assert (
            fig.data[INDEX_DATA_CHART_SPILL].hovertext[3]
            == '12:59:16 to 12:59:37 (21 sec)<br>Executor ID: <b>3</b><br>Total spilled: <b>858.5 MB</b><br>Stage: <b>1</b>'
        )
        assert (
            fig.data[INDEX_DATA_CHART_SPILL].hovertext[2]
            == '13:00:30 to 13:00:43 (12 sec)<br>Executor ID: <b>2</b><br>Total spilled: <b>3.6 GB</b><br>Stage: <b>26</b>'
        )
        
    else:
        assert (
            fig.layout.annotations[0].text
            == 'No spill, congrats!'
        )
    
    print("\nNOTHING FAILED" * 100)
