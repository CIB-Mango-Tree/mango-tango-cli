from storage import Storage, Project
from terminal_tools import draw_box, prompts, wait_for_key
from terminal_tools.inception import TerminalContext

from .analysis_main import analysis_main
from .new_analysis import new_analysis
from .select_analysis import select_analysis
from colorama import Fore


def project_main(context: TerminalContext, storage: Storage, project: Project):
  while True:
    with context.nest(draw_box(f"CIB Mango Tree/Dataset: {project.display_name}", padding_lines=0)):
      action = prompts.list_input(
        "What would you like to do?",
        choices=[
          ("New test", "new_analysis"),
          ("View a previously run test", "select_analysis"),
          ("Delete this dataset", "delete_project"),
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

    if action == "delete_project":
      confirm = prompts.confirm(
        "Are you sure you want to delete this dataset?",
        default=False
      )
      if not confirm:
        print("Deletion canceled.")
        wait_for_key(True)
        continue

      safephrase = f"DELETE {project.display_name}"
      print(f"Type {Fore.RED}{safephrase}{Fore.RESET} to confirm deletion.")
      if prompts.text(f"(type the above to confirm)") != safephrase:
        print("Deletion canceled.")
        wait_for_key(True)
        continue

      storage.delete_project(project.id)
      print("🔥 Dataset deleted.")
      wait_for_key(True)
      return
