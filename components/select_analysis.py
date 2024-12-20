from datetime import datetime
from typing import Optional

from analyzer_interface.suite import AnalyzerSuite
from storage import AnalysisModel, Project, Storage
from terminal_tools import prompts, wait_for_key
from terminal_tools.inception import TerminalContext


def select_analysis(
    context: TerminalContext, storage: Storage, suite: AnalyzerSuite, project: Project
) -> Optional[AnalysisModel]:
    now = datetime.now()
    analysis_options = sorted(
        [
            (
                analysis_label(analysis, now),
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


def analysis_label(analysis: AnalysisModel, now: datetime) -> str:
    create_time = analysis.create_time()
    timestamp_suffix = (
        " (" + present_timestamp(create_time, now) + ")"
        if create_time is not None
        else ""
    )
    return f"{analysis.display_name}{timestamp_suffix}"


def present_timestamp(timestamp: datetime, now: datetime):
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")


def present_timestamp(d: datetime, now: datetime):
    diff = now - d
    s = diff.seconds
    if diff.days > 7 or diff.days < 0:
        return d.strftime("%d %b %y")
    elif diff.days == 1:
        return "1 day ago"
    elif diff.days > 1:
        return "{} days ago".format(diff.days)
    elif s <= 1:
        return "just now"
    elif s < 60:
        return "{} seconds ago".format(s)
    elif s < 120:
        return "1 minute ago"
    elif s < 3600:
        return "{} minutes ago".format(s // 60)
    elif s < 7200:
        return "1 hour ago"
    else:
        return "{} hours ago".format(s // 3600)
