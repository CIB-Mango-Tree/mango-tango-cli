from analyzer_interface import AnalyzerSuite

from .ngram_stats import ngram_stats
from .ngrams import ngrams
from .ngram_web import ngrams_web
from .time_coordination import time_coordination
from .temporal import temporal
from .temporal_barplot import temporal_barplot
from .hashtag import hashtags

suite = AnalyzerSuite(
  all_analyzers=[
    ngrams,
    ngram_stats,
    ngrams_web,
    time_coordination,
    temporal,
    temporal_barplot
    hashtags,
  ])
