from analyzer_interface import (AnalyzerInput, AnalyzerInterface,
                                AnalyzerOutput, InputColumn, OutputColumn)

COL_AUTHOR_ID = "user_id"
COL_TIME = "time"
COL_HASHTAGS = "hashtags"

OUTPUT_GINI = "gini_coef"
OUTPUT_COL_TIMESPAN = "time_span"
OUTPUT_COL_GINI = "gini"
OUTPUT_COL_COUNT = "count"
OUTPUT_COL_HASHTAGS = COL_HASHTAGS

interface = AnalyzerInterface(
  id="hashtags",
  version="0.1.0",
  name="hashtags",
  short_description="Computes the gini coefficient over hashtag usage",
  long_description="""
    Analysis of hashtags measures the extent of online coordination among social media users
    by looking at how the usage of hashtags in messages changes over time. Specificaly,
    it measures whether certain hashtags are being used more frequently than others (i.e. trending).

    The intuition behind the analysis is that the users on social media, if coordinated by
    an event, will converge on using a few hasthags more frequently than others
    (e.g. #worldcup at the time when a soccer world cup starts). The (in)equality in
    the distritution of hasthags can be taken as evidence of coordination and is quantified
    using the Gini coefficient (see: https://ourworldindata.org/what-is-the-gini-coefficient).

    The results of this test can be used in confirmatory analyses to measure
    the extent of coordination in large datasets collected from social media platforms around
    specific events/timepoints that are hypothesized to have been coordinated.
  """,
  input=AnalyzerInput(columns=[
    InputColumn(
      name=COL_AUTHOR_ID,
      data_type="identifier",
      description="The unique identifier of the author of the message",
      name_hints=["author", "user", "poster", "username",
                  "screen_name", "screen name", "user name", "name", "email"]
    ),
    InputColumn(
      name=COL_HASHTAGS,
      data_type="text",
      description="The column containing the hashtags associated with the message",
      name_hints=["hashtags", "tags", "topics", "keywords"]
    ),
    InputColumn(
      name=COL_TIME,
      data_type="datetime",
      description="The timestamp of the message",
      name_hints=["time", "timestamp", "date", "datetime", "created", "created_at"]
    )
  ]),
  outputs=[
    AnalyzerOutput(
      id=OUTPUT_GINI,
      name="Gini coefficient over time",
      columns=[
        OutputColumn(name=OUTPUT_COL_TIMESPAN, data_type="datetime"),
        OutputColumn(name=OUTPUT_COL_GINI, data_type="float"),
        OutputColumn(name=OUTPUT_COL_COUNT, data_type="integer"),
        OutputColumn(name=OUTPUT_COL_HASHTAGS, data_type="text")
      ]
    )
  ]
)
