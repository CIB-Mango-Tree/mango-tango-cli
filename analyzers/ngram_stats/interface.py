from analyzer_interface import (AnalyzerOutput, OutputColumn,
                                SecondaryAnalyzerInterface)

from ..ngrams import interface as ngrams_interface
from ..ngrams.interface import COL_NGRAM_ID, COL_NGRAM_LENGTH, COL_NGRAM_WORDS

COL_NGRAM_TOTAL_REPS = "total_reps"
COL_NGRAM_DISTINCT_POSTER_COUNT = "distinct_posters"

OUTPUT_NGRAM_STATS = "ngram_stats"


interface = SecondaryAnalyzerInterface(
  id="ngram_stats",
  version="0.1.0",
  name="ngrams",
  short_description="",
  base_analyzer=ngrams_interface,
  autorun=True,
  outputs=[
    AnalyzerOutput(
      id=OUTPUT_NGRAM_STATS,
      name="N-gram repetition statistics",
      columns=[
        OutputColumn(name=COL_NGRAM_ID, data_type="identifier"),
        OutputColumn(name=COL_NGRAM_LENGTH, data_type="integer"),
        OutputColumn(name=COL_NGRAM_WORDS, data_type="text"),
        OutputColumn(name=COL_NGRAM_TOTAL_REPS, data_type="integer"),
        OutputColumn(name=COL_NGRAM_DISTINCT_POSTER_COUNT, data_type="integer")
      ]
    )
  ]
)
