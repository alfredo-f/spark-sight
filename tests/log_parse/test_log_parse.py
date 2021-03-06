import json
from pathlib import Path

import pandas as pd
from pandas import Timestamp

from spark_sight.log_parse.main import (
    extract_task_info,
    extract_event_stage, convert_line_to_metrics,
)
from tests.assets import assert_frame_equal_wo_order
from tests.log_parse import ROOT_TESTS_LOG_PARSE


def test_extract_event_stage():
    result = extract_event_stage(
        lines=[
            json.loads(_)
            for _ in open(
                Path(ROOT_TESTS_LOG_PARSE)
                / Path("spark_event_log__input.csv")
            ).readlines()
        ],
        stage_id=0,
    )
    
    assert result == {
        "Event": "SparkListenerStageCompleted",
        "Stage Info": {
            "Stage ID": 0,
            "Submission Time": 1648645156071,
            "Completion Time": 1648645177300
        }
    }
    

def test_extract_task_info_completed():
    lines_tasks = [
        json.loads(_)
        for _ in open(
            Path(ROOT_TESTS_LOG_PARSE)
            / Path("spark_event_log__input.csv")
        ).readlines()
    ]

    lines_tasks = [
        _
        for _ in lines_tasks
        if _["Event"] == "SparkListenerTaskEnd"
    ]
    
    result = extract_task_info(
        lines_tasks=lines_tasks,
    )

    assert_frame_equal_wo_order(
        result,
        pd.read_csv(
            Path(ROOT_TESTS_LOG_PARSE)
            / Path("extract_task_info__incomplete__expected.csv")
        ),
        cols_to_str="all",
    )


def test_convert_line_to_metrics():
    result = convert_line_to_metrics(
        {
            "Event": "SparkListenerTaskEnd", "Stage ID": 0,
            "Task Info": {
                "Task ID": 1, "Launch Time": 1648645156109,
                "Executor ID": "3", "Getting Result Time": 0,
                "Finish Time": 1648645170470,
                "Failed": False, "Killed": False
            },
            "Task Metrics": {
                "Executor Deserialize Time": 618,
                "Executor Deserialize CPU Time": 43113527,
                "Executor Run Time": 13685,
                "Executor CPU Time": 5981292588,
                "Peak Execution Memory": 369098752,
                "Result Size": 2926,
                "JVM GC Time": 433,
                "Result Serialization Time": 1,
                "Memory Bytes Spilled": 40,
                "Disk Bytes Spilled": 30,
                "Shuffle Read Metrics": {
                    "Remote Blocks Fetched": 0,
                    "Local Blocks Fetched": 0,
                    "Fetch Wait Time": 0,
                    "Remote Bytes Read": 0,
                    "Remote Bytes Read To Disk": 0,
                    "Local Bytes Read": 0,
                    "Total Records Read": 0
                },
                "Shuffle Write Metrics": {
                    "Shuffle Bytes Written": 51398083,
                    "Shuffle Write Time": 259617517,
                    "Shuffle Records Written": 2488631
                }
            }
        }
    )
    
    assert result == {
        'id_task': 1, 'id_stage': 0, 'id_executor': 3,
        'date_start': Timestamp('2022-03-30 12:59:16.108999936'),
        'date_end': Timestamp('2022-03-30 12:59:30.470000128'),
        'duration_cpu_usage': float('5981292588'),
        'duration_cpu_overhead_serde': float('44113527'),
        'duration_cpu_overhead_shuffle': float('259617517'),
        'memory_spill_disk': 30,
    }
