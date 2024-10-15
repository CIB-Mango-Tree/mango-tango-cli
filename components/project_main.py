from storage import Storage
from terminal_tools import draw_box, prompts
from terminal_tools.inception import TerminalContext

from .analysis_main import analysis_main
from .new_analysis import new_analysis
from .select_analysis import select_analysis
from .utils import ProjectInstance


def project_main(context: TerminalContext, storage: Storage, project: ProjectInstance):
  while True:
    with context.nest(draw_box(f"CIB Mango Tree/Dataset: {project.display_name}", padding_lines=0)):
      action = prompts.list_input(
        "What would you like to do?",
        choices=[
          ("New test", "new_analysis"),
          ("View a previously run test", "select_analysis"),
          ("(Back)", None),
        ],
      )

    if action is None:
      return

    if action == "new_analysis":
      analyzer = new_analysis(context, storage, project)
      if analyzer is not None:
        analysis_main(context, storage, project, analyzer)
      continue

    if action == "select_analysis":
      analyzer = select_analysis(context, storage, project)
      if analyzer is not None:
        analysis_main(context, storage, project, analyzer)
      continue
