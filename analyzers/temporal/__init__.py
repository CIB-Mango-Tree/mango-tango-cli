from analyzer_interface.interface import AnalyzerDeclaration

from .interface import interface
from .main import main

temporal = AnalyzerDeclaration(
  interface=interface,
  main=main
)
