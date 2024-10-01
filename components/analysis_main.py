
from analyzer_interface import AnalyzerInterface
from storage import Storage
from terminal_tools import (draw_box, open_directory_explorer, prompts,
                            wait_for_key)
from terminal_tools.inception import TerminalContext

from .utils import ProjectInstance


def analysis_main(context: TerminalContext, storage: Storage, project: ProjectInstance, analyzer: AnalyzerInterface):
  while True:
    with context.nest(draw_box(f"Analysis: {analyzer.name}", padding_lines=0)):
      action = prompts.list_input(
        "What would you like to do?",
        choices=[
          ("Open output directory", "open_output_dir"),
          ("Export output as...", "export_output"),
          ("(Back)", None),
        ],
      )

    if action is None:
      return

    if action == "open_output_dir":
      print("Starting file explorer")
      open_directory_explorer(
        storage._get_project_primary_output_root_path(project.id, analyzer.id)
      )
      wait_for_key(True)
      continue

    if action == "export_output":
      output_options = sorted([
        (output.name, output)
        for output in analyzer.outputs
      ], key=lambda option: option[0])
      if not output_options:
        print("There are no outputs for this analysis")
        wait_for_key(True)
        continue
      export_primary_output(context, storage, project, analyzer)


def export_primary_output(context: TerminalContext, storage: Storage, project: ProjectInstance, analyzer: AnalyzerInterface):
  while True:
    with context.nest("[Export Primary Output]\n\n"):
      output_options = sorted([
        (output.name, output)
        for output in analyzer.outputs
      ], key=lambda option: option[0])
      if not output_options:
        print("There are no outputs for this analysis")
        wait_for_key(True)
        return

      output = prompts.list_input(
        "Choose an output to export",
        choices=[
          ("(Back)", None),
          *output_options,
        ],
      )
      if output is None:
        return

      with context.nest(f"Exporting {output.name}") as scope:
        format = prompts.list_input(
          "Choose an export format",
          choices=[
            ("CSV", "csv"),
            ("Excel", "excel"),
            ("JSON", "json"),
            ("(Back)", None),
          ],
        )
        if format is None:
          continue

        scope.refresh()
        print("Beginning export...")
        output_df = storage.load_project_primary_output(
          project.id, analyzer.id, output.id)
        storage.save_project_primary_output(
          project.id, analyzer.id, output.id, output_df, format)

        scope.refresh()
        print("Exported!")
        if prompts.confirm("Would you like to open the containing directory?", default=True):
          open_directory_explorer(
            storage._get_project_primary_output_root_path(
              project.id, analyzer.id
            )
          )
          print("Directory opened")
        else:
          print("All done!")
        wait_for_key(True)
        continue
