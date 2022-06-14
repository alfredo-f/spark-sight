import json
from pathlib import Path

import dash_bootstrap_components as dbc
from dash import Dash, html, dcc, Output, Input, State
from plotly.graph_objs import Figure

from spark_sight.create_charts.parsing_spark_history_server import \
    (
    create_chart_efficiency, create_chart_stages, assign_y_to_stages,
)
from spark_sight.data_references import (
    COL_ID_STAGE,
)
from spark_sight.execute import create_dfs_for_figures, DEPLOY_MODE_CLUSTER
from tests import ROOT_TESTS


# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.


def create_charts_dash(
    path_spark_event_log,
    cpus,
    deploy_mode,
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
    
    fig_stages = Figure()
    
    fig_stages.add_trace(
        trace_timeline_stages,
    )
    
    _margin = 50

    fig_stages.update_xaxes(type="date")
    
    fig_efficiency.update_xaxes(type="date")
    
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
    
    return (
        fig_efficiency,
        fig_stages,
    )


app = Dash(
    __name__,
)


@app.callback(Output('relayout-data', 'children'),
              [Input('id-fig-efficiency', 'relayoutData')])
def display_relayout_data(relayoutData):
    return json.dumps(relayoutData, indent=2)


def _graph_event(
    select_data: dict,
    fig: dict,
):
    if select_data is not None:
        if (
            'xaxis.range[0]' in select_data
            and 'xaxis.range[1]' in select_data
        ):
            fig['layout']["xaxis"]["autorange"] = False
            
            _range = [
                select_data['xaxis.range[0]'],
                select_data['xaxis.range[1]'],
            ]
            
            fig['layout']["xaxis"]["range"] = _range
        
        elif "xaxis.autorange" in select_data:
            assert select_data["xaxis.autorange"] is True
            
            fig['layout']["xaxis"]["autorange"] = select_data[
                "xaxis.autorange"]
    
    return fig


@app.callback(
    Output('id-fig-stages', 'figure'),
    [Input('id-fig-efficiency', 'relayoutData')],
    [State('id-fig-stages', 'figure')],
)
def graph_event(select_data: dict, fig: dict):
    return _graph_event(
        select_data,
        fig,
    )


if __name__ == '__main__':
    path_spark_event_log = (
        Path(ROOT_TESTS)
        / Path("test_e2e_spill_false")
    )
    cpus = 32
    
    deploy_mode = DEPLOY_MODE_CLUSTER

    (
        fig_efficiency,
        fig_stages,
    ) = create_charts_dash(
        path_spark_event_log,
        cpus,
        deploy_mode,
    )
    
    app.layout = html.Div(
        [
            html.Pre(
                id='relayout-data',
                style={"display": "none"},
            ),
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
