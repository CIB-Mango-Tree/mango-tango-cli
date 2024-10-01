from terminal_tools import prompts, draw_box
from terminal_tools.inception import TerminalContext
from .new_project import new_project
from .select_project import select_project
from .project_main import project_main
from storage import Storage
from sys import exit


def main_menu(context: TerminalContext, storage: Storage):
  while True:
    exit_instruction = "⟪ Hit Ctrl+C at any time to exit a menu ⟫"
    with context.nest(draw_box("CIB Mango Tree") + "\n" + exit_instruction + "\n"):
      action = prompts.list_input(
        "What would you like to do?",
        choices=[
          ("New project", "new_project"),
          ("Load existing project", "load_project"),
          ("Exit", "exit"),
        ],
      )

    if action == "exit" or action is None:
      print("Bye!")
      exit(0)

    if action == "new_project":
      with context.nest(
        draw_box("CIB Mango Tree: New Project") +
          "\n" + exit_instruction + "\n"
      ):
        project = new_project(context, storage)
      if project is not None:
        project_main(context, storage, project)
      continue

    if action == "load_project":
      with context.nest(
        draw_box("CIB Mango Tree: Load Project") +
          "\n" + exit_instruction + "\n"
      ):
        project = select_project(context, storage)
      if project is not None:
        project_main(context, storage, project)
      continue
