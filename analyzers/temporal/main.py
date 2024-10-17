from datetime import timedelta

import polars as pl

from .interface import (INPUT_COL_TIMESTAMP, OUTPUT_COL_POST_COUNT,
                        OUTPUT_COL_TIME_INTERVAL_END,
                        OUTPUT_COL_TIME_INTERVAL_START,
                        OUTPUT_TABLE_INTERVAL_COUNT)

HARD_CODED_INTERVAL = timedelta(hours=1)  # For now


def main(df: pl.DataFrame):
  # Once we are able to parameterize analyzers, this can be come a parameter.
  interval = HARD_CODED_INTERVAL

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

  return {OUTPUT_TABLE_INTERVAL_COUNT: df_output}


# import numpy as np
# import plotly.express as px

# def plot_time_of_day_to_plotly(grouped_df: pl.DataFrame, TIME__INTERVAL__LENGTH: int, save_fig=False, save_method='html', filename=f'frequency_bar_graph'):


# """
# Plot the grouped Polars dataframe on Plotly with the option to export as HTML, PNG, etc.
# """

#     # Create the Plotly bar graph
#     fig = px.bar(grouped_df.to_pandas(),
#             x='time_interval',
#             y='count',
#             orientation='v',
#             title=f'Count of Records by Time of Day ({
#                 TIME__INTERVAL__LENGTH}-min intervals)',
#     labels={'time_interval': f'{
#         TIME__INTERVAL__LENGTH}-Minute Interval Label', 'count': 'Count'},
# )

#     # Show the plot
#     # fig.show() # TODO: Allow for graph export and/or upload to HTML

#     if save_fig:
#   if save_method == 'html':
#   fig.write_html(f"{filename}.html")
#   if save_method == 'png':
#   fig.write_image(f"{filename}.png")


# if __name__ == "__main__":
# CSV__INPUT= 'reddit_vm'
#     df, datetime_col_name = load_csv(CSV__INPUT)
#     df = process_datetime_feature_engineering(df, datetime_col_name)

#     TIME__INTERVAL__LENGTH = 30
#     grouped_df = analyze_time_of_day(df, TIME__INTERVAL__LENGTH)
#     plot_time_of_day_to_terminal(grouped_df, TIME__INTERVAL__LENGTH)
#     plot_time_of_day_to_plotly(grouped_df, TIME__INTERVAL__LENGTH)
#     # save_df_to_csv(grouped_df, 'time_interval_analysis') # TODO: if user elects to export to CSV
