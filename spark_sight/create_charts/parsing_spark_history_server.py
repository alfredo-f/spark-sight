import logging

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.graph_objs import Figure

from spark_sight.data_references import (
    COL_ID_STAGE,
    COL_SUBSTAGE_DURATION,
    COL_SUBSTAGE_DATE_START,
    COL_SUBSTAGE_DATE_END,
)


GANTT_XAXIS_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
GANTT_BARS_DISTANCE = 0.5
GANTT_BARS_HEIGHT = 0.4


def create_chart_efficiency(
    df: pd.DataFrame,
    fig: Figure,
    cpus_available: int,
):
    logging.info("Creating chart efficiency...")
    
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
        
        _hovertext = (
            _df[COL_SUBSTAGE_DATE_START].dt.strftime("%H:%M:%S")
            + pd.Series([" to "] * _len)
            + _df[COL_SUBSTAGE_DATE_END].dt.strftime("%H:%M:%S")
            + pd.Series([" ("] * _len)
            + _df[COL_SUBSTAGE_DURATION].map("{:.0f}".format)
            + pd.Series([" sec)"] * _len)
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
        
    fig.add_traces(
        _traces,
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

    logging.info("Creating chart efficiency: done\n")


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


def create_chart_timeline_stages(
    _df_stage,
    fig,
):
    logging.info("Creating chart timeline of stages...")
    
    stages_y = assign_y_to_stages(_df_stage)
    _df_stage.loc[:, "y"] = _df_stage[COL_ID_STAGE].astype(str).replace(stages_y)
    _df_stage.loc[:, "y_labels"] = _df_stage[COL_ID_STAGE]
    
    # Plotly can"t handle ns
    # plotly.express._core == 5.7.0, line 1681
    # args["data_frame"][args["x_end"]] = (x_end - x_start).astype("timedelta64[ms]")
    _df_stage.loc[:, COL_SUBSTAGE_DATE_START] = (
        _df_stage[COL_SUBSTAGE_DATE_START].dt.strftime(GANTT_XAXIS_DATETIME_FORMAT)
    )

    _df_stage.loc[:, COL_SUBSTAGE_DATE_END] = (
        _df_stage[COL_SUBSTAGE_DATE_END].dt.strftime(GANTT_XAXIS_DATETIME_FORMAT)
    )
    
    timeline_stages_fig = px.timeline(
        data_frame=_df_stage,
        x_start=COL_SUBSTAGE_DATE_START,
        x_end=COL_SUBSTAGE_DATE_END,
        y="y",
        text="y_labels",
    )
    
    _len = len(_df_stage)
    _start = pd.to_datetime(_df_stage[COL_SUBSTAGE_DATE_START], format=GANTT_XAXIS_DATETIME_FORMAT)
    _end = pd.to_datetime(_df_stage[COL_SUBSTAGE_DATE_END], format=GANTT_XAXIS_DATETIME_FORMAT)
    _duration = (_end - _start).astype("timedelta64[s]")
    
    _hovertext = (
        pd.Series(["Stage: <b>"] * _len)
        + _df_stage["y_labels"].map("{:.0f}".format)
        + pd.Series(["</b>"] * _len)
        + pd.Series(["<br>"] * _len)
        + _start.dt.strftime("%H:%M:%S")
        + pd.Series([" to "] * _len)
        + _end.dt.strftime("%H:%M:%S")
        + pd.Series([" ("] * _len)
        + _duration.map("{:.0f}".format)
        + pd.Series([" sec)"] * _len)
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
    
    fig.add_trace(
        timeline_stages_trace,
        row=2,
        col=1,
    )

    fig.update_xaxes(type="date")

    logging.info("Creating chart timeline of stages: done\n")
