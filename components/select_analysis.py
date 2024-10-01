from typing import Optional


from analyzer_interface import (AnalyzerInterface)
from analyzers import all_analyzers
from storage import Storage
from terminal_tools import (prompts, wait_for_key)
from terminal_tools.inception import TerminalContext

from .utils import ProjectInstance


def select_analysis(context: TerminalContext, storage: Storage, project: ProjectInstance):
  all_analyzers_by_id = {analyzer.id: analyzer for analyzer in all_analyzers}
  analysis_options: Optional[tuple[str, AnalyzerInterface]] = sorted(
    [
      (analyzer.name, analyzer)
      for analyzer_id in storage.list_project_analyses(project.id)
      if (analyzer := all_analyzers_by_id.get(analyzer_id)) is not None
    ],
    key=lambda option: option[0]
  )
  if not analysis_options:
    print("There are no analyses for this project")
    wait_for_key(True)
    return

  analyzer: Optional[AnalyzerInterface] = prompts.list_input(
    "Choose an analysis to view",
    choices=[
      ("(Back)", None),
      *analysis_options,
    ],
  )
  return analyzer
