import argparse
import json
import logging
import os.path
import sys
import warnings
from decimal import Decimal
from json import JSONDecodeError
from pathlib import Path

from plotly.subplots import make_subplots

from spark_sight.create_charts.parsing_spark_history_server import (
    create_chart_efficiency,
    create_chart_timeline_stages,
)
from spark_sight.data_references import (
    COL_ID_STAGE,
    COL_SUBSTAGE_DATE_START,
    COL_SUBSTAGE_DATE_END,
    COL_SUBSTAGE_DURATION,
    COL_STAGE_DATE_START,
    COL_STAGE_DATE_END,
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
from spark_sight.util import is_latest_version


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
    
    logging.info("Computing CPU cores available for tasks...")
    
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

    logging.info(
        "Computing CPU cores available for tasks: done"
        f". Deploy mode {DEPLOY_MODE_MAP_OUTPUT[deploy_mode]}"
        f", CPU cores available for tasks: {cpus_available}"
        "\n"
    )

    return cpus_available


def _main(
    path_spark_event_log,
    cpus: int,
    deploy_mode: str = DEPLOY_MODE_CLUSTER,
):
    lines_tasks = []
    lines_stages = []

    logging.info("Parsing Spark event log...")
    
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
                    "Parsing Spark event log: "
                    f"{_perc * 100:.0f}%"
                )
                _perc_log_index += 1

    logging.info("Parsing Spark event log: done")
    
    if len(lines_tasks) == 0 or len(lines_stages) == 0:
        logging.critical(
            compose_str_exception_file_invalid_content(
                path_spark_event_log
            )
        )
    
        return
    
    try:
        stage_ids = set(
            _["Stage Info"]["Stage ID"]
            for _ in lines_stages
            if _["Event"] == "SparkListenerStageCompleted"
        )
        
        task_info = extract_task_info(
            lines_tasks=lines_tasks,
            stage_ids=stage_ids,
        )
        
    except Exception:
        logging.critical(
            compose_str_exception_file_invalid_content(path_spark_event_log)
        )
        
        return
    
    borders_of_stages_asoftasks = determine_borders_of_stages_asoftasks(
        task_info,
    )
    
    task_info_split = split_on_borders(
        task_info,
        borders_of_stages_asoftasks,
        metrics=[
            "duration_cpu_usage",
            "duration_cpu_overhead_serde",
            "duration_cpu_overhead_shuffle",
        ],
    )

    cpus_available = determine_cpus_available(
        cpus,
        deploy_mode,
    )
    
    task_grouped = aggregate_tasks_in_substages(
        task_info_split,
        borders_of_stages_asoftasks,
        cpus_available=cpus_available,
        metrics=[
            "duration_cpu_usage",
            "duration_cpu_overhead_serde",
            "duration_cpu_overhead_shuffle",
        ],
    )

    task_grouped.loc[:, COL_SUBSTAGE_DURATION] = (
        task_grouped[COL_SUBSTAGE_DURATION]
        / float(1e9)
    )
    
    task_grouped = task_grouped[
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
    
    _charts_share = 0.2
    
    fig = make_subplots(
        rows=2,
        shared_xaxes=True,
        vertical_spacing=0.02,
        row_width=[_charts_share, 1 - _charts_share],
    )

    create_chart_efficiency(
        task_grouped,
        fig,
        cpus_available=cpus_available,
    )
    
    _df_stage = (
        create_duration_stage(
            lines_stages,
            stage_ids,
        )
        .rename(
            columns={
                COL_STAGE_DATE_START: COL_SUBSTAGE_DATE_START,
                COL_STAGE_DATE_END: COL_SUBSTAGE_DATE_END,
            }
        )
    )
    
    create_chart_timeline_stages(
        _df_stage,
        fig,
    )
    
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

    fig.update_yaxes(
        showticklabels=False,
        autorange="reversed",
        row=2,
        col=1,
    )
    
    fig.update_yaxes(
        tickformat=",.0%",
        range=[0, 1.0],
        row=1,
        col=1,
    )

    return fig


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
    
    if not is_latest_version():
        logging.info(
            "(OMG new version available: pip install --upgrade spark-sight)"
            "\n"
        )
    
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
