from typing import Optional

from analyzer_interface import AnalyzerDeclaration
from analyzers import suite
from storage import AnalysisModel, Project, Storage
from terminal_tools import prompts, wait_for_key
from terminal_tools.inception import TerminalContext


def select_analysis(
    context: TerminalContext, storage: Storage, project: Project
) -> Optional[AnalysisModel]:
    analysis_options = sorted(
        [
            (
                f"{analysis.display_name} ({analysis.create_time() or 'unknown'})",
                analysis,
            )
            for analysis in storage.list_project_analyses(project.id)
            if suite.get_primary_analyzer(analysis.primary_analyzer_id) is not None
        ],
        key=lambda option: option[0],
    )
    if not analysis_options:
        print("No tests have been run on this dataset yet.")
        wait_for_key(True)
        return None

    option: Optional[AnalysisModel] = prompts.list_input(
        "Choose a previously run test to view",
        choices=[
            ("(Back)", None),
            *analysis_options,
        ],
    )
    return option
