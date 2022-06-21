import logging
from functools import partial
from typing import List, Set, Union
from multiprocessing import Pool, cpu_count

import pandas as pd

from spark_sight.data_references import (
    COL_TASK_DATE_START,
    COL_TASK_DATE_END,
)


def _extract_task_info(
    stage_id: int,
    lines_tasks: List[dict],
):
    df = pd.DataFrame()
    
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
        
        df = pd.concat(
            [
                df,
                _task_info_df,
            ],
            ignore_index=True,
        )
    
    return df


def extract_task_info(
    lines_tasks: List[dict],
) -> pd.DataFrame:
    """Extract task information from Spark log lines.
    
    Parameters
    ----------
    lines_tasks : list of dict
        Lines of the Spark log.

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
    is_multiprocessing = True
    
    if is_multiprocessing:
        try:
            pool_number_max = cpu_count() - 1
        except NotImplementedError:
            pool_number_max = 7
    
        pool_number = min(
            len(stage_ids),
            pool_number_max,
        )
    
        with Pool(pool_number) as p:
            return pd.concat(
                (
                    p.map(
                        partial(
                            _extract_task_info,
                            lines_tasks=lines_tasks,
                        ),
                        stage_ids,
                    )
                ),
                ignore_index=True,
            ).reset_index(drop=True)
        
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    
    _log_root = "Extracting task information from Spark event log"
    _file_lines = len(lines_tasks)
    _perc_log_dict = [0.25, 0.5, 0.75]
    _perc_log_index = 0

    _task_info_df_data = []
    
    for _line_index, task in enumerate(lines_tasks):
        _task_info_dict = convert_line_to_metrics(task)
        
        _task_info_df_data.append(_task_info_dict)
        
        _perc = float(_line_index) / _file_lines
        if (
            _perc_log_index in range(len(_perc_log_dict))
            and _perc > _perc_log_dict[_perc_log_index]
        ):
            logging.info(
                f"{_log_root}: "
                f"{_perc_log_dict[_perc_log_index] * 100:.0f}%"
            )
            _perc_log_index += 1

    df = pd.DataFrame(_task_info_df_data)
    
    df = df.rename(
        columns={
            "date_start": COL_TASK_DATE_START,
            "date_end": COL_TASK_DATE_END,
        }
    )
    
    return df


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

    _date_start = (
        pd.to_datetime(
            1e6 *
            task["Task Info"]["Launch Time"]
        )
    )

    _date_end = (
        pd.to_datetime(
            1e6 *
            task["Task Info"]["Finish Time"]
        )
    )

    try:
        _memory_usage_execution = float(
            task['Task Executor Metrics']["OnHeapExecutionMemory"]
        )

    except KeyError:
        _memory_usage_execution = None

    try:
        _memory_usage_storage = float(
            task['Task Executor Metrics']["OnHeapStorageMemory"]
        )

    except KeyError:
        _memory_usage_storage = None
        
    _dict_base = {
        "id_task": task["Task Info"]["Task ID"],
        "id_stage": task["Stage ID"],
        "id_executor": int(task["Task Info"]["Executor ID"]),
        "date_start": _date_start,
        "date_end": _date_end,
        # Notice this duration is different from the green bar
        # in the Spark UI. The green bar indicates the duration
        # of the task being scheduled onto the executor,
        # not the actual execution on the CPU
        "duration_cpu_usage": float(task["Task Metrics"]["Executor CPU Time"]),
        "duration_cpu_overhead_serde": (
            float(
                task["Task Metrics"][
                    "Executor Deserialize CPU Time"]
                # / 1e9
            )
            + (
                float(
                    task["Task Metrics"][
                        "Result Serialization Time"]
                )
                * float(1e6)
            )
        ),
        "duration_cpu_overhead_shuffle": (
            # Read
            float(
                task["Task Metrics"]["Shuffle Read Metrics"][
                    "Fetch Wait Time"]
                # / 1e3
            )
            # Write
            + float(
                task["Task Metrics"]["Shuffle Write Metrics"][
                    "Shuffle Write Time"]
                # / 1e3
            )
        ),
        "memory_spill_disk": float(
            task["Task Metrics"]["Disk Bytes Spilled"]
        ),
        "memory_usage_execution": _memory_usage_execution,
        "memory_usage_storage": _memory_usage_storage,
    }
    
    return _dict_base
