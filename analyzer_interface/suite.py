from functools import cached_property
from typing import Union

from pydantic import BaseModel

from analyzer_interface.interface import (AnalyzerDeclaration,
                                          AnalyzerInterface,
                                          SecondaryAnalyzerDeclaration)


class AnalyzerSuite(BaseModel):
  all_analyzers: list[Union[AnalyzerDeclaration,
                            SecondaryAnalyzerDeclaration]]

  @cached_property
  def primary_anlyzers(self):
    return [
      analyzer for analyzer in self.all_analyzers
      if isinstance(analyzer, AnalyzerDeclaration)
    ]

  @cached_property
  def primary_analyzers_lookup(self):
    return {
      analyzer.id: analyzer
      for analyzer in self.primary_anlyzers
    }

  def get_primary_analyzer(self, analyzer_id):
    return self.primary_analyzers_lookup.get(analyzer_id)

  @cached_property
  def secondary_analyzers(self):
    return [
      analyzer for analyzer in self.all_analyzers
      if isinstance(analyzer, SecondaryAnalyzerDeclaration)
    ]

  @cached_property
  def secondary_analyzers_by_primary(self):
    return {
      analyzer.id: {
        secondary.id: secondary
        for secondary in self.secondary_analyzers
        if secondary.base_analyzer.id == analyzer.id
      }
      for analyzer in self.primary_anlyzers
    }

  def find_secondary_analyzers(self, primary_analyzer: AnalyzerInterface, *, autorun=False):
    return [
      analyzer for analyzer in self.secondary_analyzers
      if analyzer.base_analyzer.id == primary_analyzer.id
      if not autorun or analyzer.autorun
    ]

  def get_secondary_analyzer(self, analyzer_id, secondary_id):
    return self.secondary_analyzers_by_primary.get(analyzer_id, {}).get(secondary_id)
