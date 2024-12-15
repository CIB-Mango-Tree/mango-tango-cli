from datetime import timedelta

import polars as pl

from analyzer_interface.context import PrimaryAnalyzerContext

from .interface import (
    COL_TIMESTAMP,
    COL_USER_ID,
    OUTPUT_COL_FREQ,
    OUTPUT_COL_USER1,
    OUTPUT_COL_USER2,
    OUTPUT_TABLE,
)


def main(context: PrimaryAnalyzerContext):
    window_size = timedelta(minutes=15)
    step_size = timedelta(minutes=5)

    input_reader = context.input()
    df_input = input_reader.preprocess(pl.read_parquet(input_reader.parquet_path))
    df = df_input.sort(COL_TIMESTAMP)
    df = df.lazy().set_sorted(COL_TIMESTAMP)

    # Clean-up: we're not interested in rows without user ID or timestamp
    df = df.filter(
        pl.col(COL_USER_ID).is_not_null() & pl.col(COL_TIMESTAMP).is_not_null()
    )

    # Aggregate all user IDs by sliding window
    df = df.group_by_dynamic(
        pl.col(COL_TIMESTAMP), every=step_size, period=window_size
    ).agg(pl.col(COL_USER_ID).unique().alias("user_ids"))

    # Generate pairs of user IDs for each window
    df = df.explode("user_ids")
    df = df.join(df, on=COL_TIMESTAMP, how="inner").rename(
        {f"user_ids": OUTPUT_COL_USER1, f"user_ids_right": OUTPUT_COL_USER2}
    )

    df = df.group_by(OUTPUT_COL_USER1, OUTPUT_COL_USER2).agg(
        pl.len().alias(OUTPUT_COL_FREQ)
    )

    # We're most interested in highly co-occurring pairs
    df = df.sort(OUTPUT_COL_FREQ, descending=True)

    # Materialize lazy processing
    df = df.collect()
    df.write_parquet(context.output(OUTPUT_TABLE).parquet_path)
