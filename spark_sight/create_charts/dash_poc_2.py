# -*- coding: utf-8 -*-
from dash import Dash, dcc, html
import time

from dash.dependencies import Input, Output, State

app = Dash(__name__)

app.layout = html.Div(
    children=[
        html.Div(id="cls-output-1", className='output-example-loading'),
        dcc.Input(id="cls-input-1", value="Input triggers local spinner"),
        html.Div(
            [
                html.Div(id="cls-output-2", className='output-example-loading'),
                dcc.Input(id="cls-input-2", value="Input triggers nested spinner"),
            ]
        ),
    ]
)


@app.callback(Output("cls-output-1", "children"), Input("cls-input-1", "value"))
def input_triggers_spinner(value):
    time.sleep(1)
    return value


@app.callback(Output("cls-output-2", "children"), Input("cls-input-2", "value"))
def input_triggers_nested(value):
    time.sleep(1)
    return value


if __name__ == "__main__":
    app.run_server(debug=False)
