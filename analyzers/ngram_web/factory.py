from typing import Optional

import plotly.express as px
import polars as pl
from dash import Input as DashInput
from dash import Output
from dash.dcc import Graph
from dash.dcc import Input as DccInput
from dash.html import H2, Datalist, Div, Label, Option, P

from analyzer_interface.context import WebPresenterContext

from ..ngram_stats.interface import (
    COL_NGRAM_DISTINCT_POSTER_COUNT,
    COL_NGRAM_TOTAL_REPS,
    COL_NGRAM_WORDS,
    OUTPUT_NGRAM_STATS,
)
from ..ngram_stats.interface import interface as ngram_stats


def factory(context: WebPresenterContext):
    df = pl.read_parquet(
        context.dependency(ngram_stats).table(OUTPUT_NGRAM_STATS).parquet_path
    )
    all_grams = sorted(set(df[COL_NGRAM_WORDS].str.split(" ").explode()))

    @context.dash_app.callback(
        Output("scatter-plot", "figure"),
        DashInput("grams-list-input", "value"),
    )
    def update_figure(gram: Optional[str]):
        if gram is None:
            plotted_df = df
        else:
            plotted_df = df.filter(df[COL_NGRAM_WORDS].str.contains(gram))

        fig = px.scatter(
            x=plotted_df[COL_NGRAM_DISTINCT_POSTER_COUNT],
            y=plotted_df[COL_NGRAM_TOTAL_REPS]
            / plotted_df[COL_NGRAM_DISTINCT_POSTER_COUNT],
            labels={"x": "User Count", "y": "Amplifiction Factor"},
            title="User Count vs Amplification Factor",
            log_y=True,
            log_x=True,
        )

        fig.update_traces(
            hovertemplate='<b>"%{customdata}"</b><br>'
            + "User Count: %{x}<br>"
            + "Avg Repetitions Per User: %{y}<br>"
            + "<extra></extra>",
            customdata=plotted_df[COL_NGRAM_WORDS],
        )

        return fig

    fig = update_figure(None)

    context.dash_app.layout = Div(
        [
            H2("N-gram repetition statistics"),
            P(
                "Higher points are more-repeated. Higher points to the right are repeated by more users."
            ),
            P(
                [
                    Label(
                        "Search for n-grams containing: ", htmlFor="grams-list-input"
                    ),
                    DccInput(id="grams-list-input", type="text", list="grams-list"),
                ]
            ),
            Graph(id="scatter-plot", figure=fig),
            Datalist(
                id="grams-list", children=[Option(value=gram) for gram in all_grams]
            ),
        ]
    )
