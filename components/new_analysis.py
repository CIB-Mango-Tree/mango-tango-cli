from typing import Optional

import polars as pl

from analyzer_interface import (AnalyzerInterface, InputColumn,
                                UserInputColumn, column_automap,
                                get_data_type_compatibility_score)
from analyzers import all_analyzers
from storage import Storage
from terminal_tools import (draw_box, prompts,
                            wait_for_key)
from terminal_tools.inception import TerminalContext

from .utils import ProjectInstance, get_user_columns


def new_analysis(context: TerminalContext, storage: Storage, project: ProjectInstance):
  df = project.input
  with context.nest(draw_box("Choose an analysis", padding_lines=0)):
    analyzer: Optional[AnalyzerInterface] = prompts.list_input(
      "Which analysis?",
      choices=[
        ("(Back)", None),
        *(
          (f"{analyzer.name} ({analyzer.short_description})", analyzer)
          for analyzer in all_analyzers
        ),
      ],
    )

  if analyzer is None:
    return

  with context.nest(draw_box(analyzer.name, padding_lines=0)):
    with context.nest(analyzer.long_description or analyzer.short_description):
      wait_for_key(True)

    user_columns = get_user_columns(df)
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
              (f"{input_column.name} -> {user_column or '(no mapping)'}", input_column)
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
          print(f"{input_column.name} -> {user_column}")

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
      output_dfs: dict[str, pl.Dataframe] = analyzer.entry_point(
        df.select(
          pl.col(user_col).alias(input_col)
          for input_col, user_col in final_column_mapping.items()
        )
      )

      for output in analyzer.outputs:
        output_df = output_dfs[output.id]
        print(f"Output: {output.name}")
        print(output_df)

      storage.save_project_primary_outputs(project.id, analyzer.id, output_dfs)
      print("Analysis results saved")
      wait_for_key(True)
      return analyzer

    except KeyboardInterrupt:
      print("Canceled")
      wait_for_key(True)
      return None
