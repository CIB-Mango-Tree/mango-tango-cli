import polars as pl

from analyzer_interface.context import SecondaryAnalyzerContext

from ..ngrams.interface import (COL_AUTHOR_ID, COL_MESSAGE_ID,
                                COL_MESSAGE_NGRAM_COUNT, COL_NGRAM_ID,
                                COL_NGRAM_LENGTH, COL_NGRAM_WORDS,
                                OUTPUT_MESSAGE_AUTHORS, OUTPUT_MESSAGE_NGRAMS,
                                OUTPUT_NGRAM_DEFS)
from .interface import (COL_NGRAM_DISTINCT_POSTER_COUNT, COL_NGRAM_TOTAL_REPS,
                        OUTPUT_NGRAM_STATS)


def main(context: SecondaryAnalyzerContext):
  df_message_ngrams = pl.read_parquet(
    context.base.table(OUTPUT_MESSAGE_NGRAMS).parquet_path
  )
  df_ngrams = pl.read_parquet(
    context.base.table(OUTPUT_NGRAM_DEFS).parquet_path
  )
  df_message_authors = pl.read_parquet(
    context.base.table(OUTPUT_MESSAGE_AUTHORS).parquet_path
  )

  df_ngram_total_reps = (
    df_message_ngrams
      .group_by(COL_NGRAM_ID)
      .agg(pl.sum(COL_MESSAGE_NGRAM_COUNT).alias(COL_NGRAM_TOTAL_REPS))
  )

  df_ngram_distinct_posters = (
    df_message_ngrams.join(df_message_authors, on=COL_MESSAGE_ID)
      .group_by(COL_NGRAM_ID)
      .agg(pl.n_unique(COL_AUTHOR_ID).alias(COL_NGRAM_DISTINCT_POSTER_COUNT))
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

  df_ngram_summary.write_parquet(
    context.output(OUTPUT_NGRAM_STATS).parquet_path
  )
