from math import floor

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.graph_objs import Figure

from spark_sight.data_references import (
    COL_ID_STAGE,
    COL_SUBSTAGE_DURATION,
    COL_SUBSTAGE_DATE_START,
    COL_SUBSTAGE_DATE_END,
    COL_ID_EXECUTOR,
)


GANTT_XAXIS_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
GANTT_BARS_DISTANCE = 0.5
GANTT_BARS_HEIGHT = 0.4


def create_hovertext_date_range(
    _start,
    _end,
    _duration,
    _len,
):
    _duration_text = _duration.apply(format_duration_sec)
    
    return (
        _start.dt.strftime("%H:%M:%S")
        + pd.Series([" to "] * _len)
        + _end.dt.strftime("%H:%M:%S")
        + pd.Series([" ("] * _len)
        + _duration_text
        + pd.Series([")"] * _len)
    )


def create_chart_efficiency(
    df: pd.DataFrame,
):
    id_vars = [
        COL_SUBSTAGE_DATE_START,
        COL_SUBSTAGE_DATE_END,
        COL_SUBSTAGE_DURATION,
        COL_ID_STAGE,
    ]
    
    value_vars = list(df.columns[df.columns.str.startswith("efficiency__")])
    
    df_fig = df[
        id_vars + value_vars
    ]
    
    df_fig = (
        df_fig.melt(
            id_vars=id_vars,
            value_vars=value_vars,
        )
        .rename(
            columns={
                "variable": "metric",
            }
        )
    )
    
    _traces = []
    
    metrics_map_text = {
        "efficiency__duration_cpu_usage": "Actual task work",
        "efficiency__duration_cpu_overhead_serde": "Serialization, deserialization",
        "efficiency__duration_cpu_overhead_shuffle": "Shuffle read and write",
    }

    metrics_map_color = {
        "efficiency__duration_cpu_usage": "green",
        "efficiency__duration_cpu_overhead_serde": "violet",
        "efficiency__duration_cpu_overhead_shuffle": "red",
    }
    
    for _metric in df_fig["metric"].unique():
        _df = df_fig[df_fig["metric"] == _metric].copy().reset_index(drop=True)
        _y = _df["value"]
        _len = len(_y)
        _start = _df[COL_SUBSTAGE_DATE_START]
        _end = _df[COL_SUBSTAGE_DATE_END]
        _duration = _df[COL_SUBSTAGE_DURATION]
        
        _hovertext_date_range = create_hovertext_date_range(
            _start,
            _end,
            _duration,
            _len,
        )
        
        _hovertext = (
            _hovertext_date_range
            + pd.Series(["<br>"] * _len)
            + pd.Series(["<b>"] * _len)
            + pd.Series(_y.apply(lambda _: _ * 100).map("{:.1f}".format))
            + pd.Series(["%</b> ("] * _len)
            + _df["metric"].map(metrics_map_text).astype(str)
            + pd.Series([")"] * _len)
            + pd.Series(["<br>"] * _len)
            + _df[COL_ID_STAGE].map(len).map(lambda _: "Stage" + ("s" if _ > 1 else "") + ": <b>")
            + pd.Series([", ".join([str(__) for __ in _]) for _ in _df[COL_ID_STAGE].values])
            + pd.Series(["</b>"] * _len)
        )

        _hovertemplate = "<br>".join(
            [
                "%{hovertext}",
                "<extra></extra>",
            ]
        )
        
        _traces.append(
            go.Bar(
                x=_df[COL_SUBSTAGE_DATE_START],
                y=_y,
                width=_df[COL_SUBSTAGE_DURATION].apply(lambda _: float(_)) * 1e3,
                name=_metric,
                offset=0,
                hovertext=_hovertext,
                hovertemplate=_hovertemplate,
                marker_color=metrics_map_color[_metric],
            ),
        )
    
    return _traces


def assign_y_to_stages(
    _df_stages: pd.DataFrame,
):
    _df_stages = _df_stages.sort_values(COL_SUBSTAGE_DATE_START)
    y = dict()
    
    id_stage_to_see = sorted(list(set(_df_stages[COL_ID_STAGE])))
    id_stage_seen = set()
    
    while len(id_stage_to_see) > 0:
        id_stage = id_stage_to_see[0]

        id_stage_to_see = set(id_stage_to_see)
        id_stage_to_see.remove(id_stage)
        id_stage_seen.add(id_stage)
        
        stage_row = _df_stages[_df_stages[COL_ID_STAGE] == id_stage]
        
        if len(stage_row) != 1:
            raise ValueError(
                f"Stage row not found or found multiple: {stage_row}"
            )
        
        _start, _end = (
            stage_row[COL_SUBSTAGE_DATE_START].values[0],
            stage_row[COL_SUBSTAGE_DATE_END].values[0],
        )
        
        started_during_me = (
            _df_stages[
                (~_df_stages[COL_ID_STAGE].isin(id_stage_seen))
                & (
                    _start <= _df_stages[COL_SUBSTAGE_DATE_START]
                )
                & (
                    _df_stages[COL_SUBSTAGE_DATE_START] <= _end
                )
            ]
        )
        
        _concurrent_stages = [id_stage]
        
        while not started_during_me.empty:
            _concurrent_stages.extend(
                list(
                    started_during_me[COL_ID_STAGE].unique()
                )
            )

            _id_stage_seen = list(started_during_me[COL_ID_STAGE].values)
            
            for _seen in _id_stage_seen:
                id_stage_to_see.remove(_seen)
                id_stage_seen.add(_seen)

            _start, _end = (
                started_during_me[COL_SUBSTAGE_DATE_START].max(),
                started_during_me[COL_SUBSTAGE_DATE_END].max(),
            )

            started_during_me = (
                _df_stages[
                    (~_df_stages[COL_ID_STAGE].isin(id_stage_seen))
                    & (
                        _start <= _df_stages[COL_SUBSTAGE_DATE_START]
                    )
                    & (
                        _df_stages[COL_SUBSTAGE_DATE_START] <= _end
                    )
                ]
            )

        _concurrent_stages = sorted(_concurrent_stages)
        
        y = dict(
            **y,
            **{
                str(_stage): GANTT_BARS_DISTANCE * _stage_index
                for _stage_index, _stage in enumerate(_concurrent_stages)
            },
        )
        
        id_stage_to_see = sorted(list(id_stage_to_see))

    return y


def format_duration_sec(
    size: float,
) -> str:
    size = float(size)
    size_ms = size * 1000
    
    format_float_decimals = 1
    
    conversions = [
        1000,
        60,
        60,
        24,
    ]
    
    n = 0
    
    power_labels = {
        _index: _str
        for _index, _str in enumerate(
            [
                "ms",
                "sec",
                "min",
                "hours",
                "days",
            ]
        )
    }
    
    while n <= len(conversions) - 1:
        conversion = conversions[n]
        
        if size_ms < conversion:
            break
            
        else:
            size_ms /= conversion
            n += 1
            
            if n > len(conversions) - 1:
                break
    
    size_rounded = round(size_ms, format_float_decimals)
    size_decimals = size_rounded - floor(size_rounded)
    
    if n <= 1 or size_decimals == 0:
        size_str = str(
            int(round(size_rounded))
        )
        
    else:
        size_str = (
            (
                r"{:." + str(format_float_decimals) + r"f}"
            )
            .format(size_rounded)
        )
    
    return (
        size_str
        + " "
        + power_labels[n]
    )


def format_bytes(
    size: float,
) -> str:
    size = float(size)
    
    format_float_decimals = 1
    
    power = 2**10
    n = 0
    
    power_labels = {
        0: "",
        1: "K",
        2: "M",
        3: "G",
        4: "T",
    }
    
    while n <= len(power_labels) - 1:
        size /= power
        n += 1

        if size <= power or n > len(power_labels) - 1:
            break
    
    size_str = (
        (
            r"{:." + str(format_float_decimals) + r"f}"
        )
        .format(size)
    )
    
    return (
        size_str
        + " "
        + power_labels[n] + "B"
    )


def _create_trace_spill(
    df: pd.DataFrame,
    col_y: str,
    px_timeline_color_kwargs: dict = None,
    opacity: float = 1,
    is_trace_hidden: bool = False,
):
    df = df.reset_index(drop=True)
    
    # Plotly can"t handle ns
    # plotly.express._core == 5.7.0, line 1681
    # args["data_frame"][args["x_end"]] = (x_end - x_start).astype("timedelta64[ms]")
    df.loc[:, COL_SUBSTAGE_DATE_START] = (
        df[COL_SUBSTAGE_DATE_START].dt.strftime(
            GANTT_XAXIS_DATETIME_FORMAT
        )
    )
    
    df.loc[:, COL_SUBSTAGE_DATE_END] = (
        df[COL_SUBSTAGE_DATE_END].dt.strftime(GANTT_XAXIS_DATETIME_FORMAT)
    )
    
    if not is_trace_hidden:
        text_col = "memory_spill_disk_format"
        
        df.loc[:, "memory_spill_disk_format"] = (
            df["memory_spill_disk"].apply(format_bytes)
        )
        
    else:
        text_col = "y_labels"
    
    _fig = px.timeline(
        data_frame=df,
        x_start=COL_SUBSTAGE_DATE_START,
        x_end=COL_SUBSTAGE_DATE_END,
        y=col_y,
        text=text_col,
        **px_timeline_color_kwargs,
    )
    
    _len = len(df)
    _start = pd.to_datetime(
        df[COL_SUBSTAGE_DATE_START],
        format=GANTT_XAXIS_DATETIME_FORMAT
    )
    _end = pd.to_datetime(
        df[COL_SUBSTAGE_DATE_END],
        format=GANTT_XAXIS_DATETIME_FORMAT
    )
    _duration = (_end - _start).astype("timedelta64[s]")
    
    _fig.update_traces(
        opacity=opacity,
        marker_line=dict(
            width=1,
            color="white",
        ),
        textposition="inside",
        textangle=0,
        insidetextanchor="middle",
    )
    
    if not is_trace_hidden:
        
        _hovertext_date_range = create_hovertext_date_range(
            _start,
            _end,
            _duration,
            _len,
        )
        
        _hovertext_stage_all = (
            df[COL_ID_STAGE].map(len).map(
                lambda _: "Stage" + ("s" if _ > 1 else "") + ": <b>"
            )
            + pd.Series(
            [", ".join([str(__) for __ in _]) for _ in
                df[COL_ID_STAGE].values]
        )
            + pd.Series(["</b>"] * _len)
        )
        
        _hovertext = (
            _hovertext_date_range
            + pd.Series(["<br>"] * _len)
            + pd.Series(["Executor ID: <b>"] * _len)
            + df[COL_ID_EXECUTOR].astype(int).astype(str)
            + pd.Series(["</b>"] * _len)
            + pd.Series(["<br>"] * _len)
            + pd.Series(["Total spilled: <b>"] * _len)
            + df["memory_spill_disk_format"]
            + pd.Series(["</b>"] * _len)
            + pd.Series(["<br>"] * _len)
            + _hovertext_stage_all
        )
    
        _fig.update_traces(
            hovertext=_hovertext,
            hovertemplate=(
                "%{hovertext}"
                "<extra></extra>"
            ),
        )

    _trace = list(
        _fig.select_traces()
    )
    
    assert len(_trace) == 1
    return _trace[0]
    

def create_chart_spill(
    df: pd.DataFrame,
    fig,
    col_y: str,
    row: int,
    px_timeline_color_kwargs: dict = None,
    app_info: dict = None,
):
    px_timeline_color_kwargs = px_timeline_color_kwargs or {}
    
    df = df[
        df["memory_spill_disk"] > 0
    ].copy().reset_index(drop=True)
    
    if not df.empty:
        trace_not_empty = _create_trace_spill(
            df,
            col_y,
            px_timeline_color_kwargs,
        )

        fig.add_trace(
            trace_not_empty,
            row=row,
            col=1,
        )
    
    executors_all = set(app_info[COL_ID_EXECUTOR])
    
    df_empty = (
        pd.DataFrame(
            [
                {
                    COL_ID_EXECUTOR: _id_executor,
                    COL_SUBSTAGE_DATE_START: app_info[COL_SUBSTAGE_DATE_START],
                    COL_SUBSTAGE_DATE_END: app_info[COL_SUBSTAGE_DATE_START],
                }
                for _id_executor in executors_all
            ]
            + [
                {
                    COL_ID_EXECUTOR: _id_executor,
                    COL_SUBSTAGE_DATE_START: (
                        app_info[COL_SUBSTAGE_DATE_END]
                    ),
                    COL_SUBSTAGE_DATE_END: (
                        app_info[COL_SUBSTAGE_DATE_END]
                    ),
                }
                for _id_executor in executors_all
            ]
        )
    )
    
    df_empty.loc[:, "y_labels"] = ""
    
    trace_empty = _create_trace_spill(
        df_empty,
        col_y,
        px_timeline_color_kwargs={},
        opacity=0,
        is_trace_hidden=True,
    )
    
    fig.add_trace(
        trace_empty,
        row=row,
        col=1,
    )

    fig.update_xaxes(type="date")


def create_chart_stages(
    df: pd.DataFrame,
    col_y: str,
    px_timeline_color_kwargs: dict = None,
):
    px_timeline_color_kwargs = px_timeline_color_kwargs or {}
    
    if df.empty:
        raise NotImplementedError
    
    # Plotly can"t handle ns
    # plotly.express._core == 5.7.0, line 1681
    # args["data_frame"][args["x_end"]] = (x_end - x_start).astype("timedelta64[ms]")
    df.loc[:, COL_SUBSTAGE_DATE_START] = (
        df[COL_SUBSTAGE_DATE_START].dt.strftime(GANTT_XAXIS_DATETIME_FORMAT)
    )

    df.loc[:, COL_SUBSTAGE_DATE_END] = (
        df[COL_SUBSTAGE_DATE_END].dt.strftime(GANTT_XAXIS_DATETIME_FORMAT)
    )

    timeline_stages_fig = px.timeline(
        data_frame=df,
        x_start=COL_SUBSTAGE_DATE_START,
        x_end=COL_SUBSTAGE_DATE_END,
        y=col_y,
        text="y_labels",
        **px_timeline_color_kwargs,
    )
    
    _len = len(df)
    _start = pd.to_datetime(df[COL_SUBSTAGE_DATE_START], format=GANTT_XAXIS_DATETIME_FORMAT)
    _end = pd.to_datetime(df[COL_SUBSTAGE_DATE_END], format=GANTT_XAXIS_DATETIME_FORMAT)
    _duration = (_end - _start).astype("timedelta64[s]")
    
    _hovertext_date_range = create_hovertext_date_range(
        _start,
        _end,
        _duration,
        _len,
    )
    
    _hovertext = (
        pd.Series(["Stage: <b>"] * _len)
        + df["y_labels"]
        + pd.Series(["</b>"] * _len)
        + pd.Series(["<br>"] * _len)
        + _hovertext_date_range
    )

    timeline_stages_fig.update_traces(
        hovertext=_hovertext,
        hovertemplate=(
            "%{hovertext}"
            "<extra></extra>"
        ),
        marker_line=dict(
            width=1,
            color="white",
        ),
        marker_color="black",
        textposition="inside",
        textangle=0,
        insidetextanchor="middle",
    )

    timeline_stages_trace = list(
        timeline_stages_fig.select_traces()
    )

    assert len(timeline_stages_trace) == 1
    timeline_stages_trace = timeline_stages_trace[0]
    
    return timeline_stages_trace
