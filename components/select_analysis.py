from typing import Optional

from analyzer_interface import AnalyzerDeclaration
from analyzers import suite
from storage import Storage, Project
from terminal_tools import prompts, wait_for_key
from terminal_tools.inception import TerminalContext


def select_analysis(context: TerminalContext, storage: Storage, project: Project):
  analysis_options: Optional[tuple[str, AnalyzerDeclaration]] = sorted(
    [
      (analyzer.name, analyzer)
      for analyzer_id in storage.list_project_analyses(project.id)
      if (analyzer := suite.get_primary_analyzer(analyzer_id)) is not None
    ],
    key=lambda option: option[0]
  )
  if not analysis_options:
    print("No tests have been run on this dataset yet.")
    wait_for_key(True)
    return

  analyzer: Optional[AnalyzerDeclaration] = prompts.list_input(
    "Choose a previously run test to view",
    choices=[
      ("(Back)", None),
      *analysis_options,
    ],
  )
  return analyzer
