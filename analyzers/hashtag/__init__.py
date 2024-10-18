from analyzer_interface.interface import AnalyzerDeclaration

from .interface import interface
from .main import main

hashtags = AnalyzerDeclaration(
  interface=interface,
  main=main
)