import json
import pickle
from pathlib import Path
from typing import List

import dash_bootstrap_components as dbc
from dash import Dash, html, dcc, Output, Input, State
from plotly.graph_objs import Figure

from spark_sight.create_charts.parsing_spark_history_server import \
    (
    create_chart_efficiency, create_chart_stages, assign_y_to_stages,
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

    # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    # # # # # # # # # # # # # # # # # # # # # # # # # # # #
    fig_efficiency.update_layout(
        margin=dict(l=_margin, r=_margin, t=40, b=5),
        showlegend=False,
    )
    
    fig_stages.update_layout(
        margin=dict(l=_margin, r=_margin, t=40, b=5),
        showlegend=False,
    )
    
    # Scrollbar takes space
    fig_memory.update_layout(
        margin=dict(l=_margin, r=_margin, t=40, b=5),
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
    )


app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
)


class Hello:
    def __init__(
        self,
        list_output,
        list_input,
        list_state,
        relayout_data,
    ):
        self.list_output = list_output
        self.list_input = list_input
        self.list_state = list_state
        self.relayout_data = relayout_data

    def _update_xaxis_as_per_relayout(
        self,
        input_relayout: dict,
        state_fig: dict,
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
    
        return state_fig
    
    def create_method(self):
        @app.callback(
            self.list_output,
            self.list_input,
            self.list_state,
        )
        def update_xaxis_as_per_relayout(
            input_relayout: dict,
            *state_figs: dict,
        ):
            return [
                self._update_xaxis_as_per_relayout(
                    input_relayout,
                    _fig,
                )
                for _fig in state_figs
            ]
        
        return update_xaxis_as_per_relayout
    
    def create_method_2(self):
        @app.callback(
            self.relayout_data,
            self.list_input,
        )
        def display_relayout_data(relayoutData):
            return json.dumps(relayoutData, indent=2)
        
        return display_relayout_data


update_xaxis_list_fig = [
    "id-fig-efficiency",
    "id-fig-memory",
    "id-fig-stages",
]


for _index, fig_input_dashes in enumerate(update_xaxis_list_fig):
    fig_output = update_xaxis_list_fig.copy()
    fig_output.remove(fig_input_dashes)

    list_output = [
        Output(_fig, 'figure')
        for _fig in fig_output
    ].copy()

    list_input = [
        Input(fig_input_dashes, 'relayoutData'),
    ].copy()

    list_state = [
        State(_fig, 'figure')
        for _fig in fig_output
    ].copy()
    
    fig_input = fig_input_dashes.replace("-", "_")
    
    globals()[f"update_xaxis__{_index}"] = Hello(
        list_output=list_output,
        list_input=list_input,
        list_state=list_state,
        relayout_data=Output(f'relayout-data-{_index}', 'children'),
    ).create_method()

    globals()[f"relay__{_index}"] = Hello(
        list_output=list_output,
        list_input=list_input,
        list_state=list_state,
        relayout_data=Output(f'relayout-data-{_index}', 'children')
    ).create_method_2()
    
    # TODO does not work with multiple
    break


if __name__ == '__main__':
    path_spark_event_log = (
        Path(ROOT_TESTS)
        / Path("test_e2e_spill_false")
    )
    cpus = 32
    
    deploy_mode = DEPLOY_MODE_CLUSTER
    #
    # (
    #     fig_efficiency,
    #     fig_stages,
    #     fig_memory,
    # ) = create_charts_dash(
    #     path_spark_event_log,
    #     cpus,
    #     deploy_mode,
    # )
    # pickle.dump(
    #     fig_efficiency,
    #     open(
    #         r"C:\Users\a.fomitchenko\PycharmProjects\spark-sight\spark_sight\create_charts\pickle_objs\fig_efficiency",
    #         "wb"
    #     )
    # )
    # pickle.dump(
    #     fig_stages,
    #     open(
    #         r"C:\Users\a.fomitchenko\PycharmProjects\spark-sight\spark_sight\create_charts\pickle_objs\fig_stages",
    #         "wb"
    #     )
    # )
    # pickle.dump(
    #     fig_memory,
    #     open(
    #         r"C:\Users\a.fomitchenko\PycharmProjects\spark-sight\spark_sight\create_charts\pickle_objs\fig_memory",
    #         "wb"
    #     )
    # )

    (
        fig_efficiency,
        fig_stages,
        fig_memory,
    ) = (
        pickle.load(
            open(
                r"C:\Users\a.fomitchenko\PycharmProjects\spark-sight\spark_sight\create_charts\pickle_objs\fig_efficiency",
                "rb",
            )
        ),
        pickle.load(
            open(
                r"C:\Users\a.fomitchenko\PycharmProjects\spark-sight\spark_sight\create_charts\pickle_objs\fig_stages",
                "rb",
            )
        ),
        pickle.load(
            open(
                r"C:\Users\a.fomitchenko\PycharmProjects\spark-sight\spark_sight\create_charts\pickle_objs\fig_memory",
                "rb",
            )
        ),
    )
    
    layout_charts_perc_efficiency = 50
    layout_charts_perc_memory = 25
    layout_charts_perc_timeline = 25
    
    layout_charts_perc = [
        layout_charts_perc_efficiency,
        layout_charts_perc_memory,
        layout_charts_perc_timeline,
    ]
    
    layout_charts = dbc.Col(
        [
            dbc.Row(
                children=[
                    dcc.Graph(
                        id='id-fig-efficiency',
                        figure=fig_efficiency,
                    ),
                ],
                style={"height": f"{layout_charts_perc_efficiency}vh"},
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
                },
            ),
            dbc.Row(
                children=[
                    dcc.Graph(
                        id='id-fig-stages',
                        figure=fig_stages,
                    )
                ],
                style={"height": f"{layout_charts_perc_timeline}vh"},
            ),
        ],
        width=8,
    )
    
    layout_sidebar = dbc.Col(
        [
            dbc.Row(
                children=[
                    dcc.Upload(
                        id='upload-data',
                        children=html.Div(
                            [
                                'Select Spark event log',
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
        width=4,
    )
    
    app.layout = html.Div(
        children=[
            html.Pre(
                id='relayout-data-0',
                style={"display": "none"},
            ),
            html.Pre(
                id='relayout-data-1',
                style={"display": "none"},
            ),
            html.Pre(
                id='relayout-data-2',
                style={"display": "none"},
            ),
            dbc.Container(
                children=[
                    dbc.Row(
                        [
                            layout_charts,
                            layout_sidebar,
                        ],
                        style={"height": "100vh"},
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
