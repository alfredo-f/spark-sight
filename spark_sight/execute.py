import pandas as pd
from plotly.graph_objs import Figure

import argparse
import json
import logging
import os.path
import sys
import warnings
from json import JSONDecodeError
from pathlib import Path

from plotly.graph_objs import Figure
from plotly.subplots import make_subplots

from spark_sight.create_charts.parsing_spark_history_server import (
    create_chart_efficiency,
    create_chart_stages,
    assign_y_to_stages,
    create_chart_spill,
)
from spark_sight.data_references import (
    COL_ID_EXECUTOR,
)
from spark_sight.data_references import (
    COL_ID_STAGE,
    COL_SUBSTAGE_DATE_START,
    COL_SUBSTAGE_DATE_END,
    COL_SUBSTAGE_DURATION,
    COL_STAGE_DATE_START,
    COL_STAGE_DATE_END,
    COL_SUBSTAGE_DATE_INTERVAL,
)
from spark_sight.log_parse.main import (
    extract_task_info,
)
from spark_sight.log_transform.main import \
    (
    determine_borders_of_stages_asoftasks,
    split_on_borders,
    aggregate_tasks_in_substages,
    create_duration_stage,
)


warnings.simplefilter(action='ignore', category=FutureWarning)


GITHUB_REPO_ADDRESS_HOMEPAGE = "github.com/alfredo-f/spark-sight"
GITHUB_REPO_ADDRESS_ISSUES = f"{GITHUB_REPO_ADDRESS_HOMEPAGE}/issues"


def compose_str_exception_file_invalid_content(
    path_spark_event_log: str,
):
    return (
        f"Content of the provided path \n{path_spark_event_log}\n"
        "does not appear to be a valid Spark event log."
        "\nPlease check the content and retry.\n"
        ""
        f"\nIf you are positive the behavior is not expected,"
        f"\nplease raise an issue at {GITHUB_REPO_ADDRESS_ISSUES}\n"
        ""
    )


root = logging.getLogger()
root.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
root.addHandler(handler)


DEPLOY_MODE_CLUSTER = "cluster_mode"
DEPLOY_MODE_CLIENT = "client_mode"
DEPLOY_MODE_MAP_OUTPUT = {
    DEPLOY_MODE_CLUSTER: "cluster",
    DEPLOY_MODE_CLIENT: "client",
}


def determine_cpus_available(
    cpus: int,
    deploy_mode: str,
) -> int:
    if deploy_mode not in (
        DEPLOY_MODE_CLUSTER,
        DEPLOY_MODE_CLIENT,
    ):
        raise ValueError(
            f"Invalid deploy mode: {deploy_mode}"
        )
    
    cpus_available = cpus

    logging.debug(
        "Subtracting the core reserved for the OS"
    )
    cpus_available -= 1
    
    if deploy_mode == DEPLOY_MODE_CLUSTER:
        logging.debug(
            "Subtracting the core reserved for the driver"
            f" (deploy mode is {deploy_mode})"
        )
        cpus_available -= 1

    return cpus_available


def update_layout(
    fig: Figure,
    id_executor_max: int,
    cmin: float,
    cmax: float,
    df_fig_memory_empty: bool,
):
    _id_executor_max = id_executor_max + 1
    LAYOUT_EFFICIENCY_ROW = 1
    LAYOUT_TIMELINE_SPILL_ROW = 2
    LAYOUT_TIMELINE_STAGES_ROW = 3
    
    _margin = 50
    
    fig.update_layout(
        margin=dict(l=_margin, r=_margin, t=40, b=5),
        title_text="Efficiency: tasks CPU time / available CPU time of cluster",
        title_font_size=20,
        title_pad_l=_margin,
        title_pad_r=_margin,
        title_pad_t=_margin,
        title_pad_b=_margin,
        showlegend=False,
    )

    _id_executor_range = range(1, _id_executor_max)
    
    fig.update_yaxes(
        tickmode='array',
        tickvals=list(_id_executor_range),
        ticktext=[
            f"Executor {_id_executor}"
            for _id_executor in _id_executor_range
        ],
        autorange="reversed",
        row=LAYOUT_TIMELINE_SPILL_ROW,
        col=1,
    )
    
    fig.update_yaxes(
        showticklabels=False,
        autorange="reversed",
        row=LAYOUT_TIMELINE_STAGES_ROW,
        col=1,
    )
    
    fig.update_yaxes(
        tickformat=",.0%",
        range=[0, 1.0],
        row=LAYOUT_EFFICIENCY_ROW,
        col=1,
    )
    
    update_coloraxes_kwargs = dict(
        colorscale=[
            "#e5ecf6",
            "#b300ff",
        ],
        cmin=cmin,
        cmax=cmax,
        showscale=False,
    )
    
    fig.update_coloraxes(
        **update_coloraxes_kwargs
    )
    
    # TODO x in the center of period
    if df_fig_memory_empty:
        annotation_x = (
            min(fig.data[0].x) + (
                (
                    max(fig.data[0].x) - min(fig.data[0].x)
                )
                / 2
            )
        )
        
        annotation_y = (
            id_executor_max / 2
            + 0.5
        )
        
        fig.add_annotation(
            text="No spill, congrats!",
            xref="paper",
            yref="paper",
            x=annotation_x,
            y=annotation_y,
            font=dict(
                size=18,
            ),
            showarrow=False,
            row=LAYOUT_TIMELINE_SPILL_ROW,
            col=1,
        )


def create_chart_memory(df_fig_memory, fig, id_executor_max: int):
    import plotly.graph_objects as go
    
    _fig = make_subplots(
        rows=7,
        shared_xaxes=True,
    )
    
    for id_executor in range(1, id_executor_max + 1):
        _storage = df_fig_memory[
            (df_fig_memory["id_executor"] == id_executor)
            & (df_fig_memory["memory_type"] == "storage")
        ]
        
        _execution = df_fig_memory[
            (df_fig_memory["id_executor"] == id_executor)
            & (df_fig_memory["memory_type"] == "execution")
        ]
        
        _fig.add_trace(
            go.Scatter(
                x=_storage["date_end__task"], y=_storage["value"],
                mode='lines+markers',
                marker=dict(color="red"),
                line=dict(color='rgba(255, 0, 0, 0.2)'),
            ),
            row=id_executor,
            col=1,
        )
        
        _fig.add_trace(
            go.Scatter(
                x=_execution["date_end__task"], y=_execution["value"],
                mode='lines+markers',
                marker=dict(color="blue"),
                line=dict(color='rgba(0, 0, 255, 0.2)'),
            ),
            row=id_executor,
            col=1,
        )
        
    _fig.show()


def create_figure(
    df_fig_efficiency,
    df_fig_timeline_stage,
    df_fig_spill,
    df_fig_memory,
    cpus_available: int,
    app_info: dict,
):
    _charts_share_timeline = 0.2
    _charts_share_spill = 0.2
    _charts_share_efficiency = 0.6
    assert 1 == sum(
        [
            _charts_share_timeline,
            _charts_share_spill,
            _charts_share_efficiency,
        ]
    )
    
    fig = make_subplots(
        rows=3,
        shared_xaxes=True,
        vertical_spacing=0.02,
        row_width=[
            _charts_share_timeline,
            _charts_share_spill,
            _charts_share_efficiency,
        ],
    )
    
    traces_chart_efficiency = create_chart_efficiency(
        df_fig_efficiency,
    )

    fig.add_traces(
        traces_chart_efficiency,
        rows=1,
        cols=1,
    )

    fig.update_layout(
        barmode="stack",
    )

    fig.update_yaxes(
        title_text=f"CPU cores available for tasks: {cpus_available}",
        title_font_size=18,
        row=1,
        col=1,
    )
    
    stages_y = assign_y_to_stages(df_fig_timeline_stage)
    df_fig_timeline_stage.loc[:, "y"] = df_fig_timeline_stage[COL_ID_STAGE].astype(str).replace(
        stages_y
    )
    
    df_fig_timeline_stage.loc[:, "y_labels"] = (
        df_fig_timeline_stage[COL_ID_STAGE].map("{:.0f}".format)
    )

    df_fig_timeline_stage.loc[:, "fig_color"] = (
        "red"
    )
    
    df_fig_spill_empty = df_fig_spill[
        df_fig_spill["memory_spill_disk"] > 0
    ].empty
    
    cmin = 0
    
    if df_fig_spill_empty:
        cmax = 1_000_000
        
    else:
        df_fig_spill = df_fig_spill[
            df_fig_spill["memory_spill_disk"] > 0
        ].copy()

        cmax = df_fig_spill["memory_spill_disk"].max()
        
    id_executor_max = max(
        app_info[COL_ID_EXECUTOR]
    )
    
    # https://plotly.com/python/reference/layout/yaxis/#layout-yaxis-exponentformat
    create_chart_spill(
        df_fig_spill,
        fig,
        col_y=COL_ID_EXECUTOR,
        row=2,
        # Color is discarded because trace is extracted from plotly.express
        px_timeline_color_kwargs=dict(
            color="memory_spill_disk",
        ),
        app_info=app_info,
    )

    timeline_stages_trace = create_chart_stages(
        df_fig_timeline_stage,
        col_y="y",
    )

    fig.add_trace(
        timeline_stages_trace,
        row=3,
        col=1,
    )

    fig.update_xaxes(type="date")

    create_chart_memory(
        df_fig_memory,
        fig,
        id_executor_max=id_executor_max,
    )
    
    update_layout(
        fig,
        id_executor_max=id_executor_max,
        cmin=cmin,
        cmax=cmax,
        df_fig_memory_empty=df_fig_spill_empty,
    )
    
    return fig


def parse_event_log(
    path_spark_event_log: str,
):
    lines_tasks = []
    lines_stages = []

    _log_root = "Parsing Spark event log"
    
    try:
        _file = open(
            path_spark_event_log,
            "r",
        )
        _file_lines = sum(1.0 for _ in _file)
    finally:
        _file.close()
    
    _perc_log_dict = [0.25, 0.5, 0.75]
    _perc_log_index = 0
    
    with open(
        path_spark_event_log,
        "r",
    ) as _file:
        
        for _line_index, _line in enumerate(_file):
            try:
                _line = json.loads(_line)
                
                if _line["Event"] == "SparkListenerStageCompleted":
                    lines_stages.append(_line)
                elif _line["Event"] == "SparkListenerTaskEnd":
                    lines_tasks.append(_line)
            except JSONDecodeError as e:
                logging.debug(f"Invalid line : {_line}")
            
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
    
    return (
        lines_tasks,
        lines_stages,
    )


def create_df_fig_efficiency(
    task_info,
    cpus_available,
    borders_of_stages_asoftasks,
):
    metrics = [
        "duration_cpu_usage",
        "duration_cpu_overhead_serde",
        "duration_cpu_overhead_shuffle",
    ]

    task_info_split = split_on_borders(
        task_info,
        borders_all=borders_of_stages_asoftasks,
        metrics=metrics,
    )
    
    task_grouped = aggregate_tasks_in_substages(
        task_info_split,
        borders_all=borders_of_stages_asoftasks,
        metrics=metrics,
        cols_groupby=[COL_SUBSTAGE_DATE_INTERVAL],
    )
    
    task_grouped.loc[:, "duration_agg_cpu_available"] = (
        task_grouped[COL_SUBSTAGE_DURATION]
        * cpus_available
    )
    
    for _metric in metrics:
        task_grouped.loc[:, f"efficiency__{_metric}"] = (
            task_grouped[_metric]
            / task_grouped["duration_agg_cpu_available"]
        )
    
    task_grouped.loc[:, COL_SUBSTAGE_DURATION] = (
        task_grouped[COL_SUBSTAGE_DURATION]
        / float(1e9)
    )
    
    df_fig_efficiency = task_grouped[
        [
            COL_SUBSTAGE_DATE_START,
            COL_SUBSTAGE_DATE_END,
            COL_SUBSTAGE_DURATION,
            COL_ID_STAGE,
        ]
        + list(
            task_grouped.columns[
                task_grouped.columns.str.startswith("efficiency__")
            ]
        )
    ]
    
    return df_fig_efficiency


def create_df_fig_spill(
    task_info,
    borders_of_stages_asoftasks,
):
    metrics = [
        "memory_spill_disk",
    ]
    
    task_info_split = split_on_borders(
        task_info,
        borders_all=borders_of_stages_asoftasks,
        metrics=metrics,
    )
    
    task_grouped = aggregate_tasks_in_substages(
        task_info_split,
        borders_all=borders_of_stages_asoftasks,
        metrics=metrics,
        cols_groupby=[
            COL_ID_EXECUTOR,
            COL_SUBSTAGE_DATE_INTERVAL,
        ]
    )
    
    task_grouped.loc[:, COL_SUBSTAGE_DURATION] = (
        task_grouped[COL_SUBSTAGE_DURATION]
        / float(1e9)
    )
    
    return task_grouped


def parse_and_extract(
    path_spark_event_log,
):
    # TODO exceptions to handle
    _log_root = "Parsing Spark event log"
    logging.info(f"{_log_root}...")
    
    (
        lines_tasks,
        lines_stages,
    ) = parse_event_log(
        path_spark_event_log,
    )
    
    logging.info(f"{_log_root}: done\n")
    
    if len(lines_tasks) == 0 or len(lines_stages) == 0:
        logging.critical(
            compose_str_exception_file_invalid_content(
                path_spark_event_log
            )
        )
        
        return
    
    try:
        stage_ids_completed = set(
            _["Stage Info"]["Stage ID"]
                for _ in lines_stages
                if _["Event"] == "SparkListenerStageCompleted"
        )
        
        _log_root = "Extracting task information from Spark event log"
        logging.info(f"{_log_root}...")
        
        task_info = extract_task_info(
            lines_tasks=lines_tasks,
        )
        
        logging.info(f"{_log_root}: done\n")
    
    except Exception:
        logging.critical(
            compose_str_exception_file_invalid_content(path_spark_event_log)
        )
        
        return
    
    return (
        lines_stages,
        task_info,
        stage_ids_completed,
    )


def create_dfs_for_figures(
    path_spark_event_log,
    cpus: int,
    deploy_mode: str,
):
    (
        lines_stages,
        task_info,
        stage_ids_completed,
    ) = parse_and_extract(
        path_spark_event_log,
    )
    
    print("LINE CHARTSSSSSSSSSSSSSSSSSSSSSSS")
    
    _task_info = (
        task_info
            .groupby(
            ["id_executor", "date_end__task"],
            as_index=False,
        )
            .agg(
            {
                "memory_usage_storage": "max",
                "memory_usage_execution": "max",
            }
        )
            .sort_values("date_end__task").reset_index(drop=True)
    )
    
    _lines = pd.DataFrame()
    _storage = _task_info[
        ["id_executor", "memory_usage_storage",
            "date_end__task"]
    ].rename(
        columns={
            "memory_usage_storage": "value",
        }
    )
    _execution = _task_info[
        ["id_executor", "memory_usage_execution",
            "date_end__task"]
    ].rename(
        columns={
            "memory_usage_execution": "value",
        }
    )
    _storage.loc[:, "memory_type"] = "storage"
    _execution.loc[:, "memory_type"] = "execution"
    
    df_fig_memory = pd.concat(
        [
            _storage,
            _execution,
        ]
    )
    
    _log_root = "Computing CPU cores available for tasks"
    logging.info(f"{_log_root}...")
    
    cpus_available = determine_cpus_available(
        cpus,
        deploy_mode,
    )
    
    logging.info(f"{_log_root}: done\n")
    
    _log_root = "Determining borders of stages"
    logging.info(f"{_log_root}...")
    
    borders_of_stages_asoftasks = determine_borders_of_stages_asoftasks(
        task_info,
    )
    
    logging.info(f"{_log_root}: done\n")
    
    _log_root = "Creating chart of task efficiency"
    logging.info(f"{_log_root}...")
    
    df_fig_efficiency = create_df_fig_efficiency(
        task_info,
        cpus_available,
        borders_of_stages_asoftasks=borders_of_stages_asoftasks,
    )
    
    logging.info(f"{_log_root}: done\n")
    
    _log_root = "Creating chart of spill"
    logging.info(f"{_log_root}...")
    
    df_fig_spill = create_df_fig_spill(
        task_info,
        borders_of_stages_asoftasks=borders_of_stages_asoftasks,
    )
    
    df_fig_spill.loc[:, COL_ID_EXECUTOR] = (
        df_fig_spill[COL_ID_EXECUTOR].astype(float)
    )
    
    logging.info(f"{_log_root}: done\n")
    
    _log_root = "Creating chart of stage timeline"
    logging.info(f"{_log_root}...")
    
    df_fig_timeline_stage = (
        create_duration_stage(
            lines_stages,
            stage_ids_completed,
        )
            .rename(
            columns={
                COL_STAGE_DATE_START: COL_SUBSTAGE_DATE_START,
                COL_STAGE_DATE_END: COL_SUBSTAGE_DATE_END,
            }
        )
    )
    
    logging.info(f"{_log_root}: done\n")
    
    return (
        task_info,
        cpus_available,
        df_fig_efficiency,
        df_fig_timeline_stage,
        df_fig_spill,
        df_fig_memory,
    )


def _main(
    path_spark_event_log,
    cpus: int,
    deploy_mode: str = DEPLOY_MODE_CLUSTER,
):
    (
        task_info,
        cpus_available,
        df_fig_efficiency,
        df_fig_timeline_stage,
        df_fig_spill,
        df_fig_memory,
    ) = create_dfs_for_figures(
        path_spark_event_log=path_spark_event_log,
        cpus=cpus,
        deploy_mode=deploy_mode,
    )
    
    app_info = {
        **(
            task_info
            .groupby(lambda _: True)
            .agg(
                {
                    COL_ID_EXECUTOR: "unique",
                }
            )
            .to_dict(orient="records")[0]
        ),
        **(
            df_fig_timeline_stage
            .groupby(lambda _: True)
            .agg(
                {
                    COL_SUBSTAGE_DATE_START: "min",
                    COL_SUBSTAGE_DATE_END: "max",
                }
            )
            .to_dict(orient="records")[0]
        ),
    }
    
    return create_figure(
        df_fig_efficiency,
        df_fig_timeline_stage,
        df_fig_spill,
        df_fig_memory,
        cpus_available=cpus_available,
        app_info=app_info,
    )


def main(
    path_spark_event_log: str,
    cpus: int,
    deploy_mode: str = None,
):
    if deploy_mode is None:
        deploy_mode = DEPLOY_MODE_CLUSTER
    
    _path_spark_event_log = Path(path_spark_event_log)
    
    if not os.path.exists(_path_spark_event_log):
        logging.critical(
            f"You provided path \n{path_spark_event_log}\n"
            "but the file does not appear to exist"
            " on your local computer."
            "\nPlease check the spelling and retry.\n"
        )
        
        return
    
    if os.path.getsize(_path_spark_event_log) == 0:
        logging.critical(
            f"The provided path \n{path_spark_event_log}\n"
            "is an empty file."
            "\nPlease provide a valid Spark event log file.\n"
        )
        
        return
    
    fig = _main(
        path_spark_event_log=Path(path_spark_event_log),
        cpus=cpus,
        deploy_mode=deploy_mode,
    )
    
    if fig is not None:
        logging.info("Showing figure...")
        fig.show()
        logging.info("Showing figure: done\n")


def cli_check_positive(value):
    value_int = int(value)
    if value_int <= 0:
        raise argparse.ArgumentTypeError(
            f"Must be greater than zero. Provided {value_int} <= 0"
        )
    return value_int


def main_cli():
    logging.info("")
    
    logging.info(
        r"""
                      _             _       _     _
 ___ _ __   __ _ _ __| | __     ___(_) __ _| |__ | |_
/ __| '_ \ / _` | '__| |/ /____/ __| |/ _` | '_ \| __|
\__ \ |_) | (_| | |  |   <_____\__ \ | (_| | | | | |_
|___/ .__/ \__,_|_|  |_|\_\    |___/_|\__, |_| |_|\__|
    |_|                               |___/
"""
    )
    
    # if not is_latest_version():
    #     logging.info(
    #         "(OMG new version available: pip install --upgrade spark-sight)"
    #         "\n"
    #     )
    
    parser = argparse.ArgumentParser(
        description="Spark performance at a glance."
    )
    parser.add_argument(
        "--path",
        metavar="path",
        help="Local path to the Spark event log",
        dest="path_spark_event_log",
    )
    parser.add_argument(
        "--cpus",
        metavar="cpus",
        help=(
            "Total CPU cores of the cluster"
        ),
        type=cli_check_positive,
    )
    parser.add_argument(
        "--deploy_mode",
        metavar="deploy_mode",
        help=(
            "Deploy mode the Spark application was submitted with"
            ". Defaults to cluster deploy mode"
        ),
        nargs="?",
        const=DEPLOY_MODE_CLUSTER,
        choices=[
            DEPLOY_MODE_CLUSTER,
            DEPLOY_MODE_CLIENT,
        ],
    )
    
    main(
        **vars(parser.parse_args()),
    )
