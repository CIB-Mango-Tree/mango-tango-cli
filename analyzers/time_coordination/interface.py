from analyzer_interface import (AnalyzerInput, AnalyzerInterface,
                                AnalyzerOutput, InputColumn, OutputColumn)

COL_TIMESTAMP = "timestamp"
COL_USER_ID = "user_id"

OUTPUT_TABLE = "cooccurrence_frequency"
OUTPUT_COL_USER1 = "user_id_1"
OUTPUT_COL_USER2 = "user_id_2"
OUTPUT_COL_FREQ = "cooccurrence_count"


interface = AnalyzerInterface(
  id="time_coordination",
  version="0.1.0",
  name="Time Coordination",
  short_description="Identifies users that post in time-coordinated manner.",
  long_description="""
  This analysis measures time coordination between users by examining correlated user pairings. It calculates how often two users post within the same 15-minute time window, with windows sliding every 5 minutes. A high frequency of co-occurrence suggests potential coordination between the users.
  """,
  input=AnalyzerInput(columns=[
    InputColumn(
      name=COL_USER_ID,
      data_type="identifier",
      description="The unique identifier of the author of the message",
      name_hints=["author", "user", "poster", "username",
                  "screen name", "user name", "name", "email"]
    ),
    InputColumn(
      name=COL_TIMESTAMP,
      data_type="datetime",
      description="The timestamp of a message",
      name_hints=["time", "timestamp", "date", "ts"]
    )
  ]),
  outputs=[
    AnalyzerOutput(
      id=OUTPUT_TABLE,
      name="Paired poster co-occurrence count",
      columns=[
        OutputColumn(name=OUTPUT_COL_USER1, data_type="identifier"),
        OutputColumn(name=OUTPUT_COL_USER2, data_type="identifier"),
        OutputColumn(name=OUTPUT_COL_FREQ, data_type="integer")
      ]
    )
  ]
)
