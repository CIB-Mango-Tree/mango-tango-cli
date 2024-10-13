from analyzer_interface import AnalyzerSuite

from .ngram_stats import ngram_stats
from .ngrams import ngrams
from .ngram_web import ngrams_web
from .time_coordination import time_coordination

suite = AnalyzerSuite(
  all_analyzers=[
    ngrams,
    ngram_stats,
    ngrams_web,
    time_coordination
  ])
