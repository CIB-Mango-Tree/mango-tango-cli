from datetime import timedelta

import polars as pl

from analyzer_interface.context import PrimaryAnalyzerContext

from .interface import (INPUT_COL_TIMESTAMP, OUTPUT_COL_POST_COUNT,
                        OUTPUT_COL_TIME_INTERVAL_END,
                        OUTPUT_COL_TIME_INTERVAL_START,
                        OUTPUT_TABLE_INTERVAL_COUNT)

HARD_CODED_INTERVAL = timedelta(hours=1)  # For now


def main(context: PrimaryAnalyzerContext):
  # Once we are able to parameterize analyzers, this can be come a parameter.
  interval = HARD_CODED_INTERVAL

  input_reader = context.input()
  df = input_reader.preprocess(pl.read_parquet(input_reader.parquet_path))

  # Generate intervals by truncating the post timestamp to the nearest
  # specified interval within a day. This contains the start timestamp of
  # the interval each record belongs to.
  df_with_interval = df.with_columns(
    pl.col(INPUT_COL_TIMESTAMP)
      .dt.truncate(interval).dt.time()
      .alias(OUTPUT_COL_TIME_INTERVAL_START)
  )

  # Group by the interval start and count the number of posts in each interval.
  df_grouped = df_with_interval.group_by(OUTPUT_COL_TIME_INTERVAL_START).agg([
    pl.col(INPUT_COL_TIMESTAMP).count().alias(OUTPUT_COL_POST_COUNT)
  ])

  # Add the end of the interval to the output table.
  # This makes the output table self-explanatory without needing to know
  # the interval length.
  #
  # A polars pl.Time is essentially integer nanoseconds since midnight,
  # hence the 1_000_000_000 multiplier on seconds.
  df_output = df_grouped.with_columns(
    pl.col(OUTPUT_COL_TIME_INTERVAL_START)
      .cast(pl.Int64)
      .add(pl.lit(interval.total_seconds() * 1_000_000_000, dtype=pl.Int64))
      .mod(86_400_000_000_000)
      .cast(pl.Time)
      .alias(OUTPUT_COL_TIME_INTERVAL_END)
  )

  # Just re-arrange the columns and rows nicely
  df_output = df_output.select([
    OUTPUT_COL_POST_COUNT,
    OUTPUT_COL_TIME_INTERVAL_START,
    OUTPUT_COL_TIME_INTERVAL_END
  ]).sort(OUTPUT_COL_TIME_INTERVAL_START)

  df_output.write_parquet(
    context.output(OUTPUT_TABLE_INTERVAL_COUNT).parquet_path
  )
