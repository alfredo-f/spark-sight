import logging
from decimal import Decimal
from typing import List, Tuple

import pandas as pd
from numpy import datetime64

from spark_sight.data_references import (
    COL_TASK_DATE_START,
    COL_TASK_DATE_END,
    COL_ID_STAGE,
    COL_STAGE_ASOFTASKS_DATE_START,
    COL_STAGE_ASOFTASKS_DATE_END,
    COL_SUBSTAGE_DATE_START,
    COL_SUBSTAGE_DATE_END,
    COL_SUBSTAGE_DATE_INTERVAL,
    COL_SUBSTAGE_DURATION,
    COL_SPLIT_BORDERS_LIST,
    COL_STAGE_DATE_START,
    COL_STAGE_DATE_END,
    COL_STAGE_DURATION,
)
from spark_sight.log_parse.main import extract_event_stage


def check_tasks_are_split_correctly(
    df: pd.DataFrame,
) -> None:
    not_split_correcly = (
        df[
            (
                df[COL_TASK_DATE_START] <= (
                df[COL_SUBSTAGE_DATE_INTERVAL].map(
                    lambda _substage_interval: _substage_interval.left
                )
            )
            )
            | (
                df[COL_TASK_DATE_END] > (
                df[COL_SUBSTAGE_DATE_INTERVAL].map(
                    lambda _substage_interval: _substage_interval.right
                )
            )
            )
        ]
    )
    
    if not not_split_correcly.empty:
        raise ValueError(
            f"Found tasks not split correctly: \n{not_split_correcly}"
        )


def aggregate_tasks_in_substages(
    df: pd.DataFrame,
    borders_all: List[datetime64],
    cpus_available: int,
    metrics: List[str],
) -> pd.DataFrame:
    """Aggregate task metrics within the substage intervals.
    
    Sums the metrics in the substage intervals,
    and computes the efficiency based on the substage duration.
    
    A substage is the interval between two consecutive borders.
    
    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing task information already split
        across the borders.
        The id task column may not be primary key.
    borders_all : list of datetimes
        List of borders, including external borders
        of first stage start and last stage end.
    cpus_available : int
        Total number of CPU usable for task execution.
        Used to compute the efficiency
    metrics : list of str
        List of metric strings to aggregate (sum).

    Returns
    -------
    pd.DataFrame
        Aggregate information regarding tasks in substages.
        Contains:
        * substage interval
        * unique stage ids of the tasks that have been aggregated
        * aggregate metric
        * efficiency for the metric
    
    """
    logging.info(
        "Aggregating tasks within the substage intervals..."
    )

    df.loc[:, COL_SUBSTAGE_DATE_INTERVAL] = (
        pd.cut(
            df[COL_TASK_DATE_END],
            borders_all,
        )
    )

    check_tasks_are_split_correctly(df)
    
    grouped = (
        df
        .groupby(
            COL_SUBSTAGE_DATE_INTERVAL,
            as_index=False,
        )
        .agg(
            {
                COL_ID_STAGE: "unique",
                **{
                    _metric: "sum"
                    for _metric in metrics
                },
            }
        )
    )
    
    grouped = (
        grouped[
            grouped[COL_ID_STAGE].map(len) > 0
        ]
        .copy().reset_index(drop=True)
    )
    
    grouped.loc[:, COL_SUBSTAGE_DATE_START] = (
        grouped.apply(
            lambda _row: _row[COL_SUBSTAGE_DATE_INTERVAL].left,
            axis=1,
        )
    )
    
    grouped.loc[:, COL_SUBSTAGE_DATE_END] = (
        grouped.apply(
            lambda _row: _row[COL_SUBSTAGE_DATE_INTERVAL].right,
            axis=1,
        )
    )
    
    grouped.loc[:, COL_SUBSTAGE_DURATION] = (
        grouped.apply(
            (
                lambda _row: (
                    (
                        _row[COL_SUBSTAGE_DATE_END]
                        - _row[COL_SUBSTAGE_DATE_START]
                    ).delta
                )
            ),
            axis=1,
        )
    )
    
    grouped.loc[:, "duration_agg_cpu_available"] = (
        grouped[COL_SUBSTAGE_DURATION]
        * cpus_available
    )
    
    for _metric in metrics:
        grouped.loc[:, f"efficiency__{_metric}"] = (
            grouped[_metric]
            / grouped["duration_agg_cpu_available"]
        )

    logging.info(
        "Aggregating tasks within the substage intervals"
        ": done\n"
    )
    
    return grouped


def split_on_borders(
    task_info: pd.DataFrame,
    borders_all: List[datetime64],
    metrics: List[str],
) -> pd.DataFrame:
    """Split tasks running on the inner borders.
    
    Iteratively determines tasks that are running across an inner border
    of the borders input list, and replaces the dataframe row of the task
    with two rows where the task has been split in two.
    
    Parameters
    ----------
    task_info : pd.DataFrame
        Dataframe containing task information.
        The id task column is expected to be primary key.
    borders_all : list of datetimes
        List of borders, including external borders
        of first stage start and last stage end.
    metrics : list of str
        List of metric strings to split.

    Returns
    -------
    pd.DataFrame
        Dataframe where task rows of tasks on inner borders have been split.
        A new column containing the information of which borders
        have been used to apply the split is added.
        The id task column may not be primary key.
    
    """
    logging.info("Splitting tasks across borders...")
    
    threshold_metrics_discard = 1e-2
    borders_inner = borders_all[1:-1]
    df_split = task_info.copy()
    
    df_split.loc[:, COL_SPLIT_BORDERS_LIST] = pd.Series(
        [
            [] for _ in range(len(df_split))
        ]
    )
    
    for index_border, border in enumerate(borders_inner):
        logging.debug(f"Border {border}")
        
        # Remap to index of borders input list
        index_border += 1
        
        on_the_border = (
            df_split[
                (df_split[COL_TASK_DATE_START] <= border)
                & (df_split[COL_TASK_DATE_END] > border)
            ]
        )
        
        line_just_split_index = []
        line_just_split_rows_split = []
        
        for on_the_border_row in on_the_border.itertuples():
            to_split_index = getattr(on_the_border_row, "Index")
            to_split_row = on_the_border.loc[to_split_index].copy()
            
            (
                to_split_row_left,
                to_split_row_right,
            ) = split_single_border(
                border=border,
                to_split_row=to_split_row,
                metrics=metrics,
                id_task__border=(
                    f"task_{to_split_row['id_task']}"
                    f"__border_{index_border}"
                ),
            )
            
            line_just_split_index.append(
                to_split_index
            )
            
            # Discard split resulting in all metrics < threshold
            if any(
                to_split_row_left[_metric] > float(threshold_metrics_discard)
                for _metric in metrics
            ):
                line_just_split_rows_split.append(to_split_row_left)
            else:
                logging.debug(
                    f"Discarding split\n{to_split_row_left}"
                )
                
            if any(
                to_split_row_right[_metric] > float(threshold_metrics_discard)
                for _metric in metrics
            ):
                line_just_split_rows_split.append(to_split_row_right)
            else:
                logging.debug(
                    f"Discarding split\n{to_split_row_right}"
                )
                
        df_split = df_split.drop(line_just_split_index)
        
        df_split = df_split.append(
            line_just_split_rows_split,
            ignore_index=True,
        )

    logging.info("Splitting tasks across borders: done\n")
        
    return df_split


def split_row_in_two(
    id_task__border,
    to_split_row_generic,
) -> (pd.Series, pd.Series):
    to_split_row_left = to_split_row_generic.copy()
    to_split_row_left[COL_SPLIT_BORDERS_LIST] = (
        to_split_row_left[COL_SPLIT_BORDERS_LIST][:]
        + [id_task__border + "_left"]
    )

    to_split_row_right = to_split_row_generic.copy()
    to_split_row_right[COL_SPLIT_BORDERS_LIST] = (
        to_split_row_right[COL_SPLIT_BORDERS_LIST][:]
        + [id_task__border + "_right"]
    )
    
    return (
        to_split_row_left,
        to_split_row_right,
    )


def split_single_border(
    border: datetime64,
    to_split_row: pd.Series,
    metrics: List[str],
    id_task__border: str,
) -> Tuple[pd.Series, pd.Series]:
    """Split the task across the border.
    
    Parameters
    ----------
    border : datetime64
        Border across which to split.
    to_split_row : pd.Series
        Task to split.
    metrics : list of str
        List of metric strings present in the task Pandas series
        to split proportionally to the new task lengths.
    id_task__border : str
        Identifier for the new generated tasks. Contains
        original task information and border id.
    
    Returns
    -------
    pd.Series
        New task, left split.
    pd.Series
        New task, right split.
    
    """
    
    if len(metrics) == 0:
        raise ValueError("Provided empty metrics")
    
    (
        _left,
        _right,
    ) = split_row_in_two(
        id_task__border,
        to_split_row,
    )
    
    for metric in metrics:
        metric_total = _left[metric]
    
        border_left = border
        border_right = border + pd.Timedelta(1, unit="ns")
        
        reference_left = _left[COL_TASK_DATE_START]
        reference_right = _right[COL_TASK_DATE_END]
    
        duration_left = abs(reference_left - border_left)
        duration_right = abs(reference_right - border_right)
    
        duration_total_left = (
            _left[COL_TASK_DATE_END]
            - _left[COL_TASK_DATE_START]
        )

        duration_total_right = (
            _right[COL_TASK_DATE_END]
            - _right[COL_TASK_DATE_START]
        )
    
        _left[metric] = (
            metric_total
            * (
                float(duration_left.delta)
                / float(duration_total_left.delta)
            )
        )

        _right[metric] = (
            metric_total
            * (
                float(duration_right.delta)
                / float(duration_total_right.delta)
            )
        )
        
    _left[COL_TASK_DATE_END] = border_left
    _right[COL_TASK_DATE_START] = border_right
    
    return (
        _left,
        _right,
    )


def determine_borders_of_stages_asoftasks(
    task_info: pd.DataFrame,
) -> List[datetime64]:
    """Determine borders of stages as of tasks starting and ending.
    
    Determines
    * the start of a stage as the min among all tasks start dates
    * the end of a stage as the max among all tasks end dates
    
    In fact, logged stage timings are different from the associated task timings:
    
    * stage `Submission Time` <= first task `Launch Time`
    * stage `Completion Time` >= last task `Finish Time`
    
    Parameters
    ----------
    task_info : pd.DataFrame
        DataFrame of task information.

    Returns
    -------
    list
        List of borders, including external borders
        of stage start and stage end.
    
    """
    logging.info(
        "Determining borders of stages as of first and last tasks..."
    )
    
    for id_stage in task_info[COL_ID_STAGE].unique():
        logging.debug(f"Stage {id_stage}")
        
        _index_stage = task_info[COL_ID_STAGE] == id_stage
        
        # Subtracting 1 ns because we consider task belonging to interval
        # iff > stage start
        stage_asoftasks_date_start = (
            task_info.loc[_index_stage][COL_TASK_DATE_START].agg("min")
            - pd.Timedelta(1, unit="ns")
        )
        
        # Subtracting 0 ns because we consider task belonging to interval
        # iff <= stage end
        stage_asoftasks_date_end = (
            task_info.loc[_index_stage][COL_TASK_DATE_END].agg("max")
        )
        
        logging.debug(
            f"Start {stage_asoftasks_date_start}"
            f", end {stage_asoftasks_date_end}"
        )
        
        task_info.loc[_index_stage, COL_STAGE_ASOFTASKS_DATE_START] = (
            stage_asoftasks_date_start
        )
        
        task_info.loc[_index_stage, COL_STAGE_ASOFTASKS_DATE_END] = (
            stage_asoftasks_date_end
        )
    
    borders_all = set(
        list(task_info[COL_STAGE_ASOFTASKS_DATE_START].unique())
        + list(task_info[COL_STAGE_ASOFTASKS_DATE_END].unique())
    )
    
    borders_all = sorted(list(borders_all))
    
    logging.debug(f"Borders {borders_all}")
    logging.info(
        "Determining borders of stages as of first and last tasks"
        ": done\n"
    )
    
    return borders_all


def create_duration_stage(
    lines,
    stage_ids,
):
    data_stage_single = []
    
    for stage_id in stage_ids:
        stage = extract_event_stage(lines, stage_id=stage_id)
        
        stage_duration = (
            (
                stage["Stage Info"]["Completion Time"]
                - stage["Stage Info"]["Submission Time"]
            )
            / 1e3
        )
        
        # TODOish Launch time of any task related to the stage
        # can be very far from stage submission time
        start_date = pd.to_datetime(
            stage["Stage Info"]["Submission Time"] * 1e6
        )
        end_date = pd.to_datetime(stage["Stage Info"]["Completion Time"] * 1e6)
        
        data_stage_single.append(
            {
                COL_ID_STAGE: stage_id,
                COL_STAGE_DATE_START: start_date,
                COL_STAGE_DATE_END: end_date,
                COL_STAGE_DURATION: stage_duration,
            }
        )
    
    _df_stage = pd.DataFrame(data_stage_single)
    
    return _df_stage
