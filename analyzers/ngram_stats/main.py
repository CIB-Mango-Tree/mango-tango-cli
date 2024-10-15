import polars as pl

from ..ngrams.interface import (COL_AUTHOR_ID, COL_MESSAGE_ID,
                                COL_MESSAGE_NGRAM_COUNT, COL_NGRAM_ID,
                                COL_NGRAM_LENGTH, COL_NGRAM_WORDS,
                                OUTPUT_MESSAGE_AUTHORS, OUTPUT_MESSAGE_NGRAMS,
                                OUTPUT_NGRAM_DEFS)
from .interface import COL_NGRAM_DISTINCT_POSTER_COUNT, COL_NGRAM_TOTAL_REPS, OUTPUT_NGRAM_STATS


def main(ngrams_outputs: dict[str, pl.DataFrame]):
  df_message_ngrams = ngrams_outputs[OUTPUT_MESSAGE_NGRAMS]
  df_ngrams = ngrams_outputs[OUTPUT_NGRAM_DEFS]
  df_message_authors = ngrams_outputs[OUTPUT_MESSAGE_AUTHORS]

  df_ngram_total_reps = (
    df_ngrams.join(df_message_ngrams, on=COL_NGRAM_ID)
      .group_by(COL_NGRAM_ID)
      .agg(pl.sum(COL_MESSAGE_NGRAM_COUNT).alias(COL_NGRAM_TOTAL_REPS))
  )

  df_ngram_distinct_posters = (
    df_message_ngrams.join(df_message_authors, on=COL_MESSAGE_ID)
      .unique([COL_NGRAM_ID, COL_AUTHOR_ID])
      .group_by(COL_NGRAM_ID)
      .agg(pl.count(COL_AUTHOR_ID).alias(COL_NGRAM_DISTINCT_POSTER_COUNT))
  )

  df_ngram_summary = (
    df_ngrams
      .join(df_ngram_total_reps, on=COL_NGRAM_ID)
      .join(df_ngram_distinct_posters, on=COL_NGRAM_ID, how="left")
      .select(
        COL_NGRAM_ID,
        COL_NGRAM_WORDS,
        COL_NGRAM_LENGTH,
        COL_NGRAM_TOTAL_REPS,
        COL_NGRAM_DISTINCT_POSTER_COUNT
      ).sort(by=COL_NGRAM_TOTAL_REPS, descending=True)
  )

  return {OUTPUT_NGRAM_STATS: df_ngram_summary}
