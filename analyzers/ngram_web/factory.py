import polars as pl

from ..ngrams.interface import (COL_AUTHOR_ID, COL_MESSAGE_ID,
                                COL_MESSAGE_NGRAM_COUNT, COL_NGRAM_ID,
                                COL_NGRAM_LENGTH, COL_NGRAM_WORDS,
                                OUTPUT_MESSAGE_AUTHORS, OUTPUT_MESSAGE_NGRAMS,
                                OUTPUT_NGRAM_DEFS)
from dash import Dash

from dash.html import Div, H2, P
from dash.dcc import Graph
from dash.dependencies import Input, Output
import plotly.express as px


def factory(ngrams_outputs: dict[str, pl.DataFrame], dash: Dash):
  df_message_ngrams = ngrams_outputs[OUTPUT_MESSAGE_NGRAMS]
  df_ngrams = ngrams_outputs[OUTPUT_NGRAM_DEFS]
  df_message_authors = ngrams_outputs[OUTPUT_MESSAGE_AUTHORS]

  df_ngram_total_reps = (
    df_message_ngrams
      .group_by(COL_NGRAM_ID)
      .agg(pl.sum(COL_MESSAGE_NGRAM_COUNT).alias("total_reps"))
  )

  df_ngram_distinct_posters = (
    df_message_ngrams.join(df_message_authors, on=COL_MESSAGE_ID)
      .group_by(COL_NGRAM_ID)
      .agg(pl.n_unique(COL_AUTHOR_ID).alias("poster_count"))
  )

  df_ngram_summary = (
    df_ngrams
      .join(df_ngram_total_reps, on=COL_NGRAM_ID)
      .join(df_ngram_distinct_posters, on=COL_NGRAM_ID, how="left")
      .select(
        COL_NGRAM_ID,
        COL_NGRAM_WORDS,
        COL_NGRAM_LENGTH,
        "total_reps",
        "poster_count"
      ).sort(by="total_reps", descending=True)
  )

  df = df_ngram_summary
  fig = px.scatter(
    x=df["poster_count"],
    y=df["total_reps"],
    labels={"x": "Poster Count", "y": "Total Repetitions"},
    title="Poster Count vs. Total Reps",
  )

  fig.update_traces(
    hovertemplate='<b>"%{customdata}"</b><br>' +
      'Poster Count: %{x}<br>' +
      'Total Repetitions: %{y}<br>' +
      '<extra></extra>',
    customdata=df[COL_NGRAM_WORDS]
  )

  dash.layout = Div([
    H2("N-gram repetition statistics"),
    P("Higher points are more-repeated. Higher points to the left are repeated by fewer posters."),
    Graph(id="scatter-plot", figure=fig)
  ])

  return dash
