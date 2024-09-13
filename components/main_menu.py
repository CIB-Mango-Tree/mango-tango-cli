from terminal_tools import prompts, draw_box
from terminal_tools.inception import Context
from .new_analysis import new_analysis


def main_menu(context: Context):
  while True:
    exit_instruction = "⟪ Hit Ctrl+C at any time to exit a menu ⟫"
    with context.nest(draw_box("CIB Mango Tree") + "\n" + exit_instruction + "\n"):
      action = prompts.list_input(
        "What would you like to do?",
        choices=[
          ("New analysis", "new_analysis"),
          ("Exit", "exit"),
        ],
      )

    if action == "exit" or action is None:
      print("Bye!")
      exit(0)

    if action == "new_analysis":
      with context.nest(
        draw_box("CIB Mango Tree: New Analysis") +
          "\n" + exit_instruction + "\n"
      ):
        new_analysis(context)
