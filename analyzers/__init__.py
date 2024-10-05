from analyzer_interface import AnalyzerSuite

from .ngram_stats import ngram_stats
from .ngrams import ngrams

suite = AnalyzerSuite(
  all_analyzers=[
    ngrams,
    ngram_stats
  ])
