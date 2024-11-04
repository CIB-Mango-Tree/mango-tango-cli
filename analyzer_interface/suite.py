from functools import cached_property
from typing import Union

from pydantic import BaseModel

from analyzer_interface import (AnalyzerDeclaration, AnalyzerInterface,
                                SecondaryAnalyzerDeclaration,
                                SecondaryAnalyzerInterface,
                                WebPresenterDeclaration)


class AnalyzerSuite(BaseModel):
  all_analyzers: list[Union[AnalyzerDeclaration,
                            SecondaryAnalyzerDeclaration,
                            WebPresenterDeclaration]]

  @cached_property
  def primary_anlyzers(self):
    return [
      analyzer for analyzer in self.all_analyzers
      if isinstance(analyzer, AnalyzerDeclaration)
    ]

  @cached_property
  def _primary_analyzers_lookup(self):
    return {
      analyzer.id: analyzer
      for analyzer in self.primary_anlyzers
    }

  def get_primary_analyzer(self, analyzer_id):
    return self._primary_analyzers_lookup.get(analyzer_id)

  @cached_property
  def _secondary_analyzers(self):
    return [
      analyzer for analyzer in self.all_analyzers
      if isinstance(analyzer, SecondaryAnalyzerDeclaration)
    ]

  @cached_property
  def _secondary_analyzers_by_base(self):
    result: dict[str, dict[str, SecondaryAnalyzerDeclaration]] = {}
    for secondary in self._secondary_analyzers:
      base_analyzer = secondary.base_analyzer
      result.setdefault(base_analyzer.id, {}).update({secondary.id: secondary})
    return result

  def find_toposorted_secondary_analyzers(self, primary_analyzer: AnalyzerInterface) -> list[SecondaryAnalyzerDeclaration]:
    result: list[SecondaryAnalyzerDeclaration] = []
    visited_ids: set[str] = set()

    def visit(secondary_interface: SecondaryAnalyzerInterface):
      if secondary_interface.id in visited_ids:
        return
      visited_ids.add(secondary_interface.id)
      for dependency in secondary_interface.depends_on:
        visit(dependency)

      secondary_declaration = self._secondary_analyzers_by_base.get(
        primary_analyzer.id, {}).get(secondary_interface.id)
      assert secondary_declaration is not None
      result.append(secondary_declaration)

    for secondary in self._secondary_analyzers_by_base.get(primary_analyzer.id, {}).values():
      visit(secondary)
    return result

  def get_secondary_analyzer_by_id(self, analyzer_id, secondary_id):
    return self._secondary_analyzers_by_base.get(analyzer_id, {}).get(secondary_id)

  @cached_property
  def web_presenters_by_primary(self):
    return {
      analyzer.id: {
        presenter.id: presenter
        for presenter in self.all_analyzers
        if isinstance(presenter, WebPresenterDeclaration)
        if presenter.base_analyzer.id == analyzer.id
      }
      for analyzer in self.primary_anlyzers
    }

  def find_web_presenters(self, primary_analyzer: AnalyzerInterface):
    return [
      presenter for presenter in self.all_analyzers
      if isinstance(presenter, WebPresenterDeclaration)
      if presenter.base_analyzer.id == primary_analyzer.id
    ]

  def get_web_presenter(self, analyzer_id, presenter_id):
    return self.web_presenters_by_primary.get(analyzer_id, {}).get(presenter_id)
