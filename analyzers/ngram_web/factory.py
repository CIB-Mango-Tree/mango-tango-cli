import plotly.express as px
import polars as pl
from dash.dcc import Graph
from dash.html import H2, Div, P

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

    fig = px.scatter(
        x=df[COL_NGRAM_DISTINCT_POSTER_COUNT],
        y=df[COL_NGRAM_TOTAL_REPS] / df[COL_NGRAM_DISTINCT_POSTER_COUNT],
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
        customdata=df[COL_NGRAM_WORDS],
    )

    context.dash_app.layout = Div(
        [
            H2("N-gram repetition statistics"),
            P(
                "Higher points are more-repeated. Higher points to the right are repeated by more users."
            ),
            Graph(id="scatter-plot", figure=fig),
        ]
    )
