from typing import Optional

import polars as pl

from analyzer_interface import (AnalyzerInterface, InputColumn,
                                UserInputColumn, column_automap,
                                get_data_type_compatibility_score)
from analyzers import suite
from storage import Storage
from terminal_tools import (draw_box, prompts,
                            wait_for_key)
from terminal_tools.inception import TerminalContext

from .utils import ProjectInstance, get_user_columns
from .export_outputs import get_all_outputs, export_format_prompt, export_outputs_sequence


def new_analysis(context: TerminalContext, storage: Storage, project: ProjectInstance):
  df = project.input
  with context.nest(draw_box("Choose a test", padding_lines=0)):
    analyzer: Optional[AnalyzerInterface] = prompts.list_input(
      "Which test?",
      choices=[
        ("(Back)", None),
        *(
          (f"{analyzer.name} ({analyzer.short_description})", analyzer)
          for analyzer in suite.primary_anlyzers
        ),
      ],
    )

  if analyzer is None:
    return

  with context.nest(draw_box(analyzer.name, padding_lines=0)):
    with context.nest("About this test"):

      print("")
      print(analyzer.long_description or analyzer.short_description)
      print("")
      print("The test requires these columns in the input data:")
      print("")
      for input_column in analyzer.input.columns:
        print(f"** {input_column.human_readable_name_or_fallback()
                    }" + f" ({input_column.data_type})")
        print(input_column.description or "")
        print("")

      if not prompts.confirm("Do you want to proceed?", default=True):
        return

    user_columns = get_user_columns(df)
    user_columns_by_name = {
      user_column.name: user_column for user_column in user_columns
    }
    draft_column_mapping = column_automap(
      user_columns,
      analyzer.input.columns
    )

    want_to_change_mapping = False
    final_column_mapping = None
    while True:
      with context.nest("Column mapping") as column_mapping_scope:
        has_unmapped_column = any(
          draft_column_mapping.get(input_column.name, None) is None
          for input_column in analyzer.input.columns
        )

        if has_unmapped_column or want_to_change_mapping:
          selected_analyzer_column: Optional[InputColumn] = prompts.list_input(
            "Choose the column mapping to change",
            choices=[
              (
                f"{input_column.human_readable_name_or_fallback(
                )} <- {user_column or '(no mapping)'}",
                input_column
              )
              for input_column in analyzer.input.columns
              if (user_column := draft_column_mapping.get(input_column.name, None)) or True
            ],
          )

          column_mapping_scope.refresh()
          if selected_analyzer_column is None:
            break

          print(f"Mapping {selected_analyzer_column.name}")
          print(f"Type: {selected_analyzer_column.data_type}")
          print(selected_analyzer_column.description or "")
          print("")

          selected_user_column: Optional[UserInputColumn] = prompts.list_input(
            "Choose the user column",
            choices=[
              (f"{user_column.name} [{user_column.data_type}]", user_column)
              for user_column in user_columns
              if get_data_type_compatibility_score(
                selected_analyzer_column.data_type, user_column.data_type
              ) is not None
            ],
          )

          if selected_user_column is not None:
            draft_column_mapping[selected_analyzer_column.name] = selected_user_column.name

          want_to_change_mapping = False
          continue

        # At this point, every column is mapped
        for input_column in analyzer.input.columns:
          user_column = draft_column_mapping.get(input_column.name, None)
          print(f"{input_column.human_readable_name_or_fallback()} <- {user_column}")

        want_to_change_mapping = not prompts.confirm(
          "Are you happy with this mapping?",
          default=False
        )
        if not want_to_change_mapping:
          final_column_mapping = draft_column_mapping
          break

    if final_column_mapping is None:
      return

    try:
      input_df = pl.DataFrame({
        input_col:
          user_columns_by_name
            .get(user_col)
            .apply_semantic_transform()
        for input_col, user_col in final_column_mapping.items()
      })

      output_dfs: dict[str, pl.Dataframe] = analyzer.entry_point(input_df)

      storage.save_project_primary_outputs(project.id, analyzer.id, output_dfs)
      print("Base analysis for the test finished")

      for secondary in suite.find_secondary_analyzers(analyzer, autorun=True):
        print("Running post-analysis: ", secondary.name)
        secondary_output_dfs = secondary.entry_point(output_dfs)
        storage.save_project_secondary_outputs(
          project.id, analyzer.id, secondary.id, secondary_output_dfs)
        print(f"Post-analysis {secondary.name} finished")

      outputs = get_all_outputs(storage, project, analyzer)
      print("")
      print("You now have the option to export the following outputs:")
      for output in outputs:
        print(output.name)
      print("")

      export_format = export_format_prompt()
      if export_format is None:
        print("No problem. You can also export outputs later from the analysis menu.")
        wait_for_key(True)
      else:
        export_outputs_sequence(
          storage, project, analyzer, outputs, export_format
        )

      return analyzer

    except KeyboardInterrupt:
      print("Canceled")
      wait_for_key(True)
      return None
