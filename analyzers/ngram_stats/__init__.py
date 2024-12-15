from analyzer_interface import SecondaryAnalyzerDeclaration

from .interface import interface
from .main import main

ngram_stats = SecondaryAnalyzerDeclaration(interface=interface, main=main)
