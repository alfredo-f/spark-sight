import dash_bootstrap_components as dbc
# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

from pathlib import Path

import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc
from plotly.graph_objs import Figure

from spark_sight.create_charts.parsing_spark_history_server import \
    (
    create_chart_efficiency, create_chart_stages, assign_y_to_stages,
)
from spark_sight.data_references import (
    COL_ID_EXECUTOR, COL_ID_STAGE,
)
from spark_sight.data_references import (
    COL_SUBSTAGE_DATE_START,
    COL_SUBSTAGE_DATE_END,
)
from spark_sight.execute import create_dfs_for_figures, DEPLOY_MODE_CLUSTER
from tests import ROOT_TESTS




app = Dash(__name__)




if __name__ == '__main__':
    path_spark_event_log = (
        Path(ROOT_TESTS)
        / Path("test_e2e_spill_false")
    )
    cpus = 32
    
    deploy_mode = DEPLOY_MODE_CLUSTER
    
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
    
    stages_y = assign_y_to_stages(df_fig_timeline_stage)
    df_fig_timeline_stage.loc[:, "y"] = df_fig_timeline_stage[
        COL_ID_STAGE].astype(
        str
    ).replace(
        stages_y
    )
    
    df_fig_timeline_stage.loc[:, "y_labels"] = (
        df_fig_timeline_stage[COL_ID_STAGE].map("{:.0f}".format)
    )
    
    df_fig_timeline_stage.loc[:, "fig_color"] = (
        "red"
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
    
    traces_chart_efficiency = create_chart_efficiency(
        df_fig_efficiency,
    )
    
    trace_timeline_stages = create_chart_stages(
        df_fig_timeline_stage,
        col_y="y",
    )
    
    # assume you have a "long-form" data frame
    # see https://plotly.com/python/px-arguments/ for more options
    
    fig_efficiency = Figure()
    
    fig_efficiency.add_traces(
        traces_chart_efficiency,
    )
    
    fig_efficiency.update_layout(
        barmode="stack",
    )
    
    fig_efficiency.update_yaxes(
        title_text=f"CPU cores available for tasks: {cpus_available}",
        title_font_size=18,
    )
    
    fig_stages = Figure()
    
    fig_stages.add_trace(
        trace_timeline_stages,
    )
    
    fig_stages.update_xaxes(type="date")

    LAYOUT_EFFICIENCY_ROW = 1
    LAYOUT_TIMELINE_SPILL_ROW = 2
    LAYOUT_TIMELINE_STAGES_ROW = 3

    _margin = 50

    fig_efficiency.update_layout(
        margin=dict(l=_margin, r=_margin, t=40, b=5),
        showlegend=False,
    )
    
    fig_stages.update_layout(
        margin=dict(l=_margin, r=_margin, t=40, b=5),
        showlegend=False,
    )

    fig_stages.update_yaxes(
        showticklabels=False,
        autorange="reversed",
    )

    fig_efficiency.update_yaxes(
        tickformat=",.0%",
        range=[0, 1.0],
    )
    
    # Remove margin dash
    
    app.layout = html.Div(
        [
            dbc.Col(
                [
                    dbc.Row(
                        [
                            dcc.Graph(
                                id='id-fig-efficiency',
                                figure=fig_efficiency,
                                style={"height": "50vh"},
                            ),
                        ],
                    ),
                    dbc.Row(
                        [
                            dcc.Graph(
                                id='id-fig-stages',
                                figure=fig_stages,
                                style={"height": "50vh"},
                            )
                        ],
                    )
                ],
            ),
        ],
        style={"height": "100vh"},
    )
    app.run_server(debug=False)
