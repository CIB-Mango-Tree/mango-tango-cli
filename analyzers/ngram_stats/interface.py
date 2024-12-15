from analyzer_interface import AnalyzerOutput, OutputColumn, SecondaryAnalyzerInterface

from ..ngrams import interface as ngrams_interface
from ..ngrams.interface import (
    COL_AUTHOR_ID,
    COL_MESSAGE_ID,
    COL_MESSAGE_NGRAM_COUNT,
    COL_MESSAGE_SURROGATE_ID,
    COL_MESSAGE_TEXT,
    COL_MESSAGE_TIMESTAMP,
    COL_NGRAM_ID,
    COL_NGRAM_LENGTH,
    COL_NGRAM_WORDS,
)

COL_NGRAM_TOTAL_REPS = "total_reps"
COL_NGRAM_REPS_PER_USER = "reps_per_user"
COL_NGRAM_DISTINCT_POSTER_COUNT = "distinct_posters"

OUTPUT_NGRAM_STATS = "ngram_stats"
OUTPUT_NGRAM_FULL = "ngram_full"


interface = SecondaryAnalyzerInterface(
    id="ngram_stats",
    version="0.1.0",
    name="Copy-Pasta Detector",
    short_description="",
    base_analyzer=ngrams_interface,
    outputs=[
        AnalyzerOutput(
            id=OUTPUT_NGRAM_STATS,
            name="N-gram repetition statistics",
            columns=[
                OutputColumn(name=COL_NGRAM_ID, data_type="identifier"),
                OutputColumn(name=COL_NGRAM_LENGTH, data_type="integer"),
                OutputColumn(name=COL_NGRAM_WORDS, data_type="text"),
                OutputColumn(name=COL_NGRAM_TOTAL_REPS, data_type="integer"),
                OutputColumn(name=COL_NGRAM_DISTINCT_POSTER_COUNT, data_type="integer"),
            ],
        ),
        AnalyzerOutput(
            id=OUTPUT_NGRAM_FULL,
            name="N-gram full report",
            columns=[
                OutputColumn(
                    name=COL_NGRAM_ID,
                    data_type="identifier",
                    human_readable_name="ngram ID",
                ),
                OutputColumn(
                    name=COL_NGRAM_LENGTH,
                    data_type="integer",
                    human_readable_name="ngram length",
                ),
                OutputColumn(
                    name=COL_NGRAM_WORDS,
                    data_type="text",
                    human_readable_name="ngram content",
                ),
                OutputColumn(
                    name=COL_NGRAM_TOTAL_REPS,
                    data_type="integer",
                    human_readable_name="ngram frequency",
                ),
                OutputColumn(
                    name=COL_NGRAM_DISTINCT_POSTER_COUNT,
                    data_type="integer",
                    human_readable_name="distinct user count",
                ),
                OutputColumn(
                    name=COL_AUTHOR_ID,
                    data_type="identifier",
                    human_readable_name="unique username",
                ),
                OutputColumn(
                    name=COL_NGRAM_REPS_PER_USER,
                    data_type="integer",
                    human_readable_name="frequency by user",
                ),
                OutputColumn(
                    name=COL_MESSAGE_SURROGATE_ID,
                    data_type="identifier",
                    human_readable_name="UPN",
                ),
                OutputColumn(
                    name=COL_MESSAGE_ID,
                    data_type="identifier",
                    human_readable_name="post identifier",
                ),
                OutputColumn(
                    name=COL_MESSAGE_TEXT,
                    data_type="text",
                    human_readable_name="post content",
                ),
                OutputColumn(
                    name=COL_MESSAGE_TIMESTAMP,
                    data_type="datetime",
                    human_readable_name="timestamp",
                ),
            ],
        ),
    ],
)
