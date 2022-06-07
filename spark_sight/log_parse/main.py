import logging
from typing import List, Set, Union

import pandas as pd

from spark_sight.data_references import (
    COL_TASK_DATE_START,
    COL_TASK_DATE_END,
)


def extract_task_info(
    lines_tasks: List[dict],
    stage_ids: Union[List[int], Set[int]],
) -> pd.DataFrame:
    """Extract task information from Spark log lines.
    
    Parameters
    ----------
    lines_tasks : list of dict
        Lines of the Spark log.
    stage_ids : iterable of int
        Stage ids to filter for.

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame containing task information.
        It contains the following columns:
        
        * id_task: int
        * id_stage: int
        * date_start__task: start date of the task
        * date_end__task: end date of the task
        * duration_cpu_usage: duration of actual work. Measured in ns
        * duration_cpu_overhead_serde: duration of overhead (de)serialization. Measured in ns
        * duration_cpu_overhead_shuffle: duration of overhead shuffle (reading and writing). Measured in ns

    """
    _log_root = "Extracting task information from Spark event log"

    _perc_log_dict = [0.25, 0.5, 0.75]
    _perc_log_index = 0
    _perc_log_len = len(stage_ids)

    df = None
    
    for stage_id_index, stage_id in enumerate(stage_ids):
        logging.debug(f"Stage {stage_id}")
        
        tasks = extract_events_tasks(lines_tasks, stage_id=stage_id)
        
        for task in tasks:
            _task_info_dict = convert_line_to_metrics(task)
            
            _task_info_df = pd.json_normalize(
                _task_info_dict,
                sep="_",
            )
            
            _task_info_df = _task_info_df.rename(
                columns={
                    "date_start": COL_TASK_DATE_START,
                    "date_end": COL_TASK_DATE_END,
                }
            )
            
            if df is None:
                df = _task_info_df
            else:
                df = pd.concat(
                    [
                        df,
                        _task_info_df,
                    ],
                    ignore_index=True,
                )
        
        _perc = float(stage_id_index) / _perc_log_len
        if (
            _perc_log_index in range(len(_perc_log_dict))
            and _perc > _perc_log_dict[_perc_log_index]
        ):
            logging.info(
                f"{_log_root}: "
                f"{_perc_log_dict[_perc_log_index] * 100:.0f}%"
            )
            _perc_log_index += 1
        
    return df


def extract_events_tasks(
    lines: List[dict],
    stage_id: int,
) -> List[dict]:
    """Extract information related to the stage.
    
    Extracts events `SparkListenerTaskEnd` for the stage
    
    Parameters
    ----------
    lines : list of dict
        Lines of the Spark log.
    stage_id : int
        Stage id.

    Returns
    -------
    list
        List of task events.

    """
    tasks = [
        _ for _ in lines
        if (
            _["Event"] == "SparkListenerTaskEnd"
            and _["Stage ID"] == stage_id
        )
    ]
    
    return tasks


def extract_event_stage(
    lines: List[dict],
    stage_id: int,
) -> dict:
    """Extract information related to the stage.

    Extracts event `SparkListenerStageCompleted`.

    Parameters
    ----------
    lines : list of dict
        Lines of the Spark log.
    stage_id : int
        Stage id.

    Returns
    -------
    dict
        Stage event.

    """
    stage = [
        _ for _ in lines
        if (
            _["Event"] == "SparkListenerStageCompleted"
            and _["Stage Info"]["Stage ID"] == stage_id
        )
    ]
    
    if len(stage) != 1:
        raise ValueError(f"Stage info no exact match {stage}")
    
    return stage[0]


def convert_line_to_metrics(
    task,
) -> dict:
    """Convert Spark log line to nested dict of metrics.
    
    Parameters
    ----------
    task : dict
        Line of the Spark log.

    Returns
    -------
    dict
        Nested dict of task metrics.

    """
    
    # https://spark.apache.org/docs/latest/monitoring.html#executor-task-metrics
    # "Task Metrics" > "Executor CPU Time" does not include time of deserialization,
    # see notes_1.png

    _dict_base = {
        "id": {
            "task": task["Task Info"]["Task ID"],
            "stage": task["Stage ID"],
            "executor": int(task["Task Info"]["Executor ID"]),
        },
        "date": {
            "start": task["Task Info"]["Launch Time"],
            "end": task["Task Info"]["Finish Time"],
        },
        "duration": {
            "cpu": {
                "usage":
                    # Notice this duration is different from the green bar
                    # in the Spark UI. The green bar indicates the duration
                    # of the task being scheduled onto the executor,
                    # not the actual execution on the CPU
                    float(task["Task Metrics"]["Executor CPU Time"]),
                "overhead": {
                    "serde":
                        float(
                            task["Task Metrics"][
                                "Executor Deserialize CPU Time"]
                            # / 1e9
                        )
                        + (
                            float(task["Task Metrics"][
                                "Result Serialization Time"]
                            ) * float(1e6)
                        ),
                    "shuffle": (
                        # "read":
                        float(
                            task["Task Metrics"]["Shuffle Read Metrics"][
                                "Fetch Wait Time"]
                            # / 1e3
                        )
                        # "write":
                        + float(
                            task["Task Metrics"]["Shuffle Write Metrics"][
                                "Shuffle Write Time"]
                            # / 1e3
                        )
                    )
                }
            }
        },
        "memory": {
            "spill": {
                "disk": float(
                    task["Task Metrics"]["Disk Bytes Spilled"]
                ),
                # "memory": float(
                #     task["Task Metrics"]["Memory Bytes Spilled"]
                # ),
            }
        },
    }
    
    _dict_base["date"]["start"] = (
        pd.to_datetime(
            1e6 *
            _dict_base["date"]["start"]
        )
    )
    
    _dict_base["date"]["end"] = (
        pd.to_datetime(
            1e6 *
            _dict_base["date"]["end"]
        )
    )
    
    return _dict_base
