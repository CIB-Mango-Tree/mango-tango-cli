from analyzer_interface import (
    AnalyzerInput,
    AnalyzerInterface,
    AnalyzerOutput,
    InputColumn,
    OutputColumn,
)

INPUT_COL_TIMESTAMP = "timestamp"

OUTPUT_TABLE_INTERVAL_COUNT = "interval_count"
OUTPUT_COL_TIME_INTERVAL_START = "time_interval_start"
OUTPUT_COL_TIME_INTERVAL_END = "time_interval_end"
OUTPUT_COL_POST_COUNT = "count"

description = """
This analysis breaks down timestamped data into granular components like hour, minute, and time of day, then groups events into custom time intervals (e.g., every 60 minutes) to analyze activity patterns. It helps you pinpoint when events occur and aggregates them into labeled time blocks, allowing for easy visualization and comparison.

The input is a dataset with timestamps, which is transformed into time-based features like "minute of the day" and grouped into intervals. The output is a summary table showing how often events occur in each time block, with clearly labeled intervals (e.g., "08:00-09:00").

For CIB analysis, this approach helps by revealing temporal patterns that may indicate coordinated activity. By visualizing when spikes in activity happen, you can detect irregularities or patterns, such as posts clustering at specific times, which could signal automated behavior or coordinated inauthentic efforts.
"""

interface = AnalyzerInterface(
    id="temporal",
    version="0.1.0",
    name="Time frequency analysis",
    short_description="Counts posting events in custom time intervals to discover potential periodic activity.",
    long_description=description,
    input=AnalyzerInput(
        columns=[
            InputColumn(
                name=INPUT_COL_TIMESTAMP,
                human_readable_name="Post Timestamp",
                data_type="datetime",
                description="The timestamp of the event or post.",
            )
        ]
    ),
    outputs=[
        AnalyzerOutput(
            id=OUTPUT_TABLE_INTERVAL_COUNT,
            name="Interval event count",
            description="The count of events in each time interval.",
            columns=[
                OutputColumn(
                    name=OUTPUT_COL_TIME_INTERVAL_START,
                    description="The start timestamp of the bin interval",
                    data_type="time",
                ),
                OutputColumn(
                    name=OUTPUT_COL_TIME_INTERVAL_END,
                    description="The end timestamp of the bin interval",
                    data_type="time",
                ),
                OutputColumn(
                    name=OUTPUT_COL_POST_COUNT,
                    description="The number of posts that fall within the time interval",
                    data_type="integer",
                ),
            ],
        )
    ],
)
