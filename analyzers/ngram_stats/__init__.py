from analyzer_interface import SecondaryAnalyzerDeclaration
from .main import main
from .interface import interface

ngram_stats = SecondaryAnalyzerDeclaration(
  interface=interface,
  main=main
)
