from datetime import datetime
from typing import Optional

from app import AnalysisContext, ProjectContext
from terminal_tools import prompts, wait_for_key


def select_analysis(proj: ProjectContext) -> Optional[AnalysisContext]:
    now = datetime.now()
    analysis_options = sorted(
        [
            (
                analysis_label(analysis, now),
                analysis,
            )
            for analysis in proj.list_analyses()
        ],
        key=lambda option: option[0],
    )
    if not analysis_options:
        print("No tests have been run on this dataset yet.")
        wait_for_key(True)
        return None

    option: Optional[AnalysisContext] = prompts.list_input(
        "Choose a previously run test to view",
        choices=[
            ("(Back)", None),
            *analysis_options,
        ],
    )
    return option


def analysis_label(analysis: AnalysisContext, now: datetime) -> str:
    timestamp_suffix = (
        " (" + present_timestamp(analysis.create_time, now) + ")"
        if analysis.create_time is not None
        else ""
    )
    return f"{analysis.display_name}{timestamp_suffix}"


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
