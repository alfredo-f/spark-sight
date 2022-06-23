import re
import json
import pickle
from pathlib import Path
from typing import List

import dash_bootstrap_components as dbc
from dash import Dash, html, dcc, Output, Input, State, ctx
from plotly.graph_objs import Figure

from spark_sight.create_charts.parsing_spark_history_server import \
    (
    create_chart_efficiency,
    create_chart_stages,
    assign_y_to_stages,
    create_chart_spill,
)
from spark_sight.data_references import (
    COL_ID_STAGE,
    COL_ID_EXECUTOR,
    COL_SUBSTAGE_DATE_START,
    COL_SUBSTAGE_DATE_END,
)
from spark_sight.execute import (
    create_dfs_for_figures,
    DEPLOY_MODE_CLUSTER,
    create_chart_memory,
)
from spark_sight.util import configure_pandas
from tests import ROOT_TESTS


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
    
    id_executor_max = int(
        max(app_info[COL_ID_EXECUTOR])
    )

    fig_spill = Figure()

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

    (
        spill_trace_not_empty,
        spill_trace_empty,
    ) = create_chart_spill(
        df_fig_spill,
        col_y=COL_ID_EXECUTOR,
        # Color is discarded because trace is extracted from plotly.express
        px_timeline_color_kwargs=dict(
            color="memory_spill_disk",
        ),
        app_info=app_info,
    )
    
    if spill_trace_not_empty is not None:
        fig_spill.add_trace(
            spill_trace_not_empty,
        )

    fig_spill.add_trace(
        spill_trace_empty,
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

    fig_spill.update_coloraxes(
        **update_coloraxes_kwargs
    )

    # TODO x in the center of period
    if df_fig_spill_empty:
        _data_0_x = fig_spill.data[0].x
        
        annotation_x = (
            min(_data_0_x) + (
            (
                max(_data_0_x) - min(_data_0_x)
            )
            / 2
        )
        )
    
        annotation_y = (
            id_executor_max / 2
            + 0.5
        )
    
        fig_spill.add_annotation(
            text="No spill, congrats!",
            xref="paper",
            yref="paper",
            x=annotation_x,
            y=annotation_y,
            font=dict(
                size=18,
            ),
            showarrow=False,
        )

    _id_executor_max = id_executor_max + 1

    _id_executor_range = range(1, _id_executor_max)
    
    fig_spill.update_yaxes(
        tickmode='array',
        tickvals=list(_id_executor_range),
        ticktext=[
            f"Executor {_id_executor}"
            for _id_executor in _id_executor_range
        ],
        autorange="reversed",
    )
    
    fig_memory = create_chart_memory(
        df_fig_memory,
        id_executor_max=id_executor_max,
    )

    _margin = 50
    
    # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    fig_stages.update_xaxes(type="date")
    
    fig_efficiency.update_xaxes(type="date")

    fig_memory.update_xaxes(type="date")
    
    fig_spill.update_xaxes(type="date")

    # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    fig_efficiency.update_layout(
        margin=dict(l=_margin, r=_margin),
        showlegend=False,
    )
    
    fig_stages.update_layout(
        margin=dict(l=_margin, r=_margin),
        showlegend=False,
    )
    
    # Scrollbar takes space
    fig_memory.update_layout(
        margin=dict(l=_margin, r=_margin),
        showlegend=False,
    )

    # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    fig_stages.update_yaxes(
        showticklabels=False,
        autorange="reversed",
    )
    
    fig_efficiency.update_yaxes(
        tickformat=",.0%",
        range=[0, 1.0],
    )

    fig_memory.update_yaxes(
        exponentformat="SI",
    )
    
    return (
        fig_efficiency,
        fig_stages,
        fig_memory,
        fig_spill,
    )


app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
)


def _update_xaxis_as_per_relayout(
    input_relayout: dict,
    state_fig: dict,
    state_fig_name: str,
):
    if input_relayout is None:
        return state_fig
    
    if (
        'xaxis.range[0]' in input_relayout
        and 'xaxis.range[1]' in input_relayout
    ):
        _range = [
            input_relayout['xaxis.range[0]'],
            input_relayout['xaxis.range[1]'],
        ]
        
        for _xaxis in [
            _key
            for _key in state_fig['layout']
            if _key.startswith("xaxis")
        ]:
            state_fig['layout'][_xaxis]["autorange"] = False
            state_fig['layout'][_xaxis]["range"] = _range
    
    elif "xaxis.autorange" in input_relayout:
        assert input_relayout["xaxis.autorange"] is True
        
        for _xaxis in [
            _key
            for _key in state_fig['layout']
            if _key.startswith("xaxis")
        ]:
            state_fig['layout'][_xaxis]["autorange"] = (
                input_relayout["xaxis.autorange"]
            )
            
        for _yaxis in [
            _key
            for _key in state_fig['layout']
            if _key.startswith("yaxis")
        ]:
            if state_fig_name == "id-fig-efficiency":
                state_fig['layout'][_yaxis]["autorange"] = (
                    False
                )
                state_fig['layout'][_yaxis]["range"] = [0, 1]
                
            else:
                state_fig['layout'][_yaxis]["autorange"] = (
                    True
                )

    return state_fig


update_xaxis_list_fig = [
    "id-fig-efficiency",
    "id-fig-memory",
    "id-fig-stages",
    "id-fig-spill",
]


list_output = [
    Output(_fig, 'figure')
    for _fig in update_xaxis_list_fig
]

list_state = [
    State(_fig, 'figure')
    for _fig in update_xaxis_list_fig
]

list_input = [
    Input(_fig, 'relayoutData')
    for _fig in update_xaxis_list_fig
]


@app.callback(
    list_output,
    list_input,
    list_state,
)
def display_relayout_data(
    *args,
):
    # TODO prevent_initial_call attribute of Dash callbacks.
    assert len(args) == len(list_input) + len(list_state)
    
    input_relayout_data = args[:len(list_input)]
    state_figs = args[len(list_input):len(list_input) + len(list_state)]
    
    input_relayout_data_dedup = []
    
    xaxis_duplicate_regex = re.compile(r"xaxis\d+\.range\[([01])\]")
    
    button_id = ctx.triggered_id if not None else 'No clicks yet'
    
    input_relayout = (
        None
        if button_id is None
        else input_relayout_data[update_xaxis_list_fig.index(button_id)]
    )

    if input_relayout is not None:
        xaxis_duplicate = [
            _key
            for _key in input_relayout
            if xaxis_duplicate_regex.match(_key)
        ]
    
        if len(xaxis_duplicate) == 0:
            input_relayout_data_dedup.append(
                input_relayout
            )
    
        else:
        
            for _xaxis_duplicate in xaxis_duplicate:
                start_or_end = xaxis_duplicate_regex.findall(_xaxis_duplicate)
                assert len(start_or_end) == 1, start_or_end
                start_or_end = start_or_end[0]
            
                if (
                    input_relayout[_xaxis_duplicate]
                    != input_relayout[f"xaxis.range[{start_or_end}]"]
                ):
                    raise ValueError(
                        input_relayout[_xaxis_duplicate],
                        input_relayout[f"xaxis.range[{start_or_end}]"],
                    )
            
                else:
                    input_relayout.pop(_xaxis_duplicate)

    state_figs = [
        _update_xaxis_as_per_relayout(
            input_relayout,
            _fig,
            state_fig_name=update_xaxis_list_fig[state_figs.index(_fig)],
        )
        for _fig in state_figs
    ]
    
    return state_figs


if __name__ == '__main__':
    import pandas as pd
    configure_pandas(pd)
    
    # path_spark_event_log = (
    #     Path(ROOT_TESTS)
    #     / Path("test_e2e_spill_true")
    # )
    # cpus = 32
    
    path_spark_event_log = (
        r"C:\Users\a.fomitchenko\Downloads\spark-application-1655200731376.inprogress"
    )
    cpus = 20

    # path_spark_event_log = (
    #     r"C:\Users\a.fomitchenko\Downloads\spark-application-1655885940796.inprogress"
    # )
    # cpus = 60
    
    deploy_mode = DEPLOY_MODE_CLUSTER
    
    (
        fig_efficiency,
        fig_stages,
        fig_memory,
        fig_spill,
    ) = create_charts_dash(
        path_spark_event_log,
        cpus,
        deploy_mode,
    )
    pickle.dump(
        fig_efficiency,
        open(
            r"C:\Users\a.fomitchenko\PycharmProjects\spark-sight\spark_sight\create_charts\pickle_objs\fig_efficiency",
            "wb"
        )
    )
    pickle.dump(
        fig_stages,
        open(
            r"C:\Users\a.fomitchenko\PycharmProjects\spark-sight\spark_sight\create_charts\pickle_objs\fig_stages",
            "wb"
        )
    )
    pickle.dump(
        fig_memory,
        open(
            r"C:\Users\a.fomitchenko\PycharmProjects\spark-sight\spark_sight\create_charts\pickle_objs\fig_memory",
            "wb"
        )
    )
    pickle.dump(
        fig_spill,
        open(
            r"C:\Users\a.fomitchenko\PycharmProjects\spark-sight\spark_sight\create_charts\pickle_objs\fig_spill",
            "wb"
        )
    )
    #
    # (
    #     fig_efficiency,
    #     fig_stages,
    #     fig_memory,
    #     fig_spill,
    # ) = (
    #     pickle.load(
    #         open(
    #             r"C:\Users\a.fomitchenko\PycharmProjects\spark-sight\spark_sight\create_charts\pickle_objs\fig_efficiency",
    #             "rb",
    #         )
    #     ),
    #     pickle.load(
    #         open(
    #             r"C:\Users\a.fomitchenko\PycharmProjects\spark-sight\spark_sight\create_charts\pickle_objs\fig_stages",
    #             "rb",
    #         )
    #     ),
    #     pickle.load(
    #         open(
    #             r"C:\Users\a.fomitchenko\PycharmProjects\spark-sight\spark_sight\create_charts\pickle_objs\fig_memory",
    #             "rb",
    #         )
    #     ),
    #     pickle.load(
    #         open(
    #             r"C:\Users\a.fomitchenko\PycharmProjects\spark-sight\spark_sight\create_charts\pickle_objs\fig_spill",
    #             "rb",
    #         )
    #     ),
    # )
    
    layout_charts_perc_efficiency = 30
    layout_charts_perc_spill = 25
    layout_charts_perc_memory = 50
    layout_charts_perc_timeline = 20
    
    # assert sum(
    #     [
    #         layout_charts_perc_efficiency,
    #         layout_charts_perc_spill,
    #         layout_charts_perc_memory,
    #         layout_charts_perc_timeline,
    #     ]
    # ) == 100
    
    layout_charts_perc = [
        layout_charts_perc_efficiency,
        layout_charts_perc_spill,
        layout_charts_perc_memory,
        layout_charts_perc_timeline,
    ]
    
    margins_align_to_spill = {
        "margin-right": "19px",
        "margin-left": "19px",
    }
    
    width_sidebar = 2
    
    layout_charts = dbc.Col(
        [
            dbc.Row(
                children=[
                    dcc.Graph(
                        id='id-fig-efficiency',
                        figure=fig_efficiency,
                    ),
                ],
                style={
                    "height": f"{layout_charts_perc_efficiency}vh",
                    **margins_align_to_spill,
                },
            ),
            dbc.Row(
                children=[
                    dcc.Graph(
                        id='id-fig-spill',
                        figure=fig_spill,
                    ),
                ],
                style={"height": f"{layout_charts_perc_spill}vh"},
            ),
            dbc.Row(
                id="id-fig-memory-row",
                children=[
                    html.Div(
                        children=[
                            dcc.Graph(
                                id='id-fig-memory',
                                figure=fig_memory,
                            ),
                        ],
                    ),
                ],
                style={
                    "height": f"{layout_charts_perc_memory}vh",
                    "overflow-y": "scroll",
                    **margins_align_to_spill,
                },
            ),
            dbc.Row(
                children=[
                    dcc.Graph(
                        id='id-fig-stages',
                        figure=fig_stages,
                    )
                ],
                style={
                    "height": f"{layout_charts_perc_timeline}vh",
                    **margins_align_to_spill,
                },
            ),
        ],
        width=12 - width_sidebar,
    )
    
    layout_sidebar = dbc.Col(
        [
            dbc.Row(
                children=[
                    dcc.Upload(
                        id='upload-data',
                        children=html.Div(
                            [
                                'Select log',
                            ],
                        ),
                        style={
                            'width': '100%',
                            'height': '60px',
                            'lineHeight': '60px',
                            'borderWidth': '1px',
                            'borderStyle': 'dashed',
                            'borderRadius': '5px',
                            'textAlign': 'center',
                            'margin': '10px',
                        },
                        multiple=False,
                    ),
                ],
            ),
        ],
        width=width_sidebar,
    )
    
    app.layout = html.Div(
        children=[
            # html.Div(
            #     className="output-example-loading",
            #     id="id-loading",
            #     style={
            #         "position": "fixed",
            #         "height": "100vh",
            #         "width": "100vw",
            #         "z-index": "1",
            #     }
            # ),
            html.Pre(
                id='relayout-data',
                style={"display": "none"},
            ),
            dbc.Container(
                children=[
                    dbc.Row(
                        [
                            layout_charts,
                            layout_sidebar,
                        ],
                        style={
                            "height": "100vh",
                        },
                    ),
                ],
                fluid=True,
                style={
                    "height": "100vh",
                    "width": "100vw",
                },
            ),
        ],
        style={
            "height": "100vh",
            "width": "100vw",
        },
    )
    
    app.run_server(debug=False)
