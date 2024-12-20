import re
from itertools import accumulate
from typing import Optional

import plotly.express as px
import plotly.graph_objects as go
import polars as pl
from dash import Input as DashInput
from dash import Output
from dash.dcc import Graph
from dash.dcc import Input as DccInput
from dash.dcc import RadioItems
from dash.html import H2, Datalist, Div, Em, Label, Option, P

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
    explanation_total = "N-grams to the right are repeated by more users. N-grams higher up are repeated more times overall."
    explanation_amplification = "N-grams to the right are repeated by more users. N-grams higher up are repeated more times on average per user."

    @context.dash_app.callback(
        [Output("scatter-plot", "figure"), Output("explanation", "children")],
        [DashInput("grams-list-input", "value"), DashInput("y-axis", "value")],
    )
    def update_figure(filter_text: Optional[str], y_axis: str):
        y_label = (
            "Total Repetition"
            if y_axis == "total_repetition"
            else "Amplification Factor"
        )
        y_legend_label = (
            "Avg Repetitions Per User"
            if y_axis == "amplification_factor"
            else "Total Repetition (All Users)"
        )
        explanation = (
            explanation_total
            if y_axis == "total_repetition"
            else explanation_amplification
        )

        matcher = create_word_matcher(filter_text or "", pl.col(COL_NGRAM_WORDS))
        plotted_df = df.filter(matcher) if matcher is not None else df
        if plotted_df.height == 0:
            fig = go.Figure()
            fig.update_layout(
                xaxis_title="User Count",
                yaxis_title=y_label,
                annotations=[
                    {
                        "text": "No matching n-grams found",
                        "x": 0.5,
                        "y": 0.5,
                        "xref": "paper",
                        "yref": "paper",
                        "showarrow": False,
                    }
                ],
            )
            return fig, explanation

        x_value = plotted_df[COL_NGRAM_DISTINCT_POSTER_COUNT]
        y_value = (
            plotted_df[COL_NGRAM_TOTAL_REPS]
            if y_axis == "total_repetition"
            else plotted_df[COL_NGRAM_TOTAL_REPS]
            / plotted_df[COL_NGRAM_DISTINCT_POSTER_COUNT]
        )

        fig = px.scatter(
            x=x_value,
            y=y_value,
            labels={"x": "User Count", "y": y_label},
            log_y=True,
            log_x=True,
        )

        fig.update_traces(
            hovertemplate='<b>"%{customdata}"</b><br>'
            + "User Count: %{x}<br>"
            + y_legend_label
            + ": %{y}<br>"
            + "<extra></extra>",
            customdata=plotted_df[COL_NGRAM_WORDS],
        )

        return fig, explanation

    fig = update_figure(None, "total_repetition")

    context.dash_app.layout = Div(
        style={"display": "flex", "flex-direction": "column", "height": "100%"},
        children=[
            Div(
                [
                    H2("N-gram repetition statistics"),
                    P(
                        [
                            "Show on Y-axis: ",
                            RadioItems(
                                id="y-axis",
                                options=[
                                    {
                                        "label": "Total Repetition",
                                        "value": "total_repetition",
                                    },
                                    {
                                        "label": "Amplification Factor",
                                        "value": "amplification_factor",
                                    },
                                ],
                                value="total_repetition",
                                inline=True,
                                style={"display": "inline-block"},
                            ),
                        ]
                    ),
                    P(Em(id="explanation", children=explanation_total)),
                    P(
                        [
                            Label(
                                "Search for n-grams containing: ",
                                htmlFor="grams-list-input",
                            ),
                            Datalist(
                                id="grams-list",
                                children=[Option(value=gram) for gram in all_grams],
                            ),
                            DccInput(
                                id="grams-list-input", type="text", list="grams-list"
                            ),
                        ]
                    ),
                ]
            ),
            Graph(
                id="scatter-plot",
                figure=fig,
                style={"height": "300px", "flex-grow": "1"},
            ),
        ],
    )


def create_word_matcher(subject: str, col: pl.Expr) -> Optional[pl.Expr]:
    subject = subject.strip().lower()
    words = re.split(r"[^\w]", subject)
    words = [word for word in words if word]
    if not words:
        return None
    return accumulate(
        (col.str.contains("(^|[^\\w])" + re.escape(word)) for word in words),
        lambda a, b: a & b,
    )
