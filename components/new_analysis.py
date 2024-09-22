import csv
import os

import pandas as pd
from pandas.core.dtypes.base import ExtensionDtype

from preprocessing.series_semantic import infer_series_semantic
from terminal_tools import prompts, wait_for_key, draw_box
from terminal_tools.inception import Context


def new_analysis(context: Context):
  with context.nest(draw_box("1. Data Source", padding_lines=0)):
    print("Select a file for your analysis")
    selected_file = prompts.file_selector("Select a file")
    if selected_file is None:
      print("Canceled")
      return wait_for_key(True)

    print(f"Selected file: {selected_file}")
    confirm_file = prompts.confirm("Is this correct?", default=True)
    if not confirm_file:
      print("Canceled")
      return wait_for_key(True)

    file_extension: str = os.path.splitext(selected_file)[1].lower()
    if file_extension == ".csv":
      try:
        with context.nest("Reading CSV file..."):
          print("Opening file...")
          with open(selected_file, "r", encoding="utf8") as file:
            dialect = csv.Sniffer().sniff(file.read(65536))

          df = pd.read_csv(
            selected_file,
            delimiter=dialect.delimiter,
            quotechar=dialect.quotechar,
            header="infer"
          )

      except Exception as e:
        print(f"Error reading CSV file: {e}")
        wait_for_key(True)
        return

    else:
      print(
        f"Unsupported file type: {file_extension or '(file with no extension)'}"
      )
      wait_for_key(True)
      return

  with context.nest(draw_box("2. Data preview", padding_lines=0)):
    print(df)
    for col in df.columns:
      semantic = infer_series_semantic(df[col])
      print(
        f"Column semantic: {col} - {semantic.semantic_name if semantic else present_column_type(df.dtypes[col])}"
      )

    wait_for_key(True)

  with context.nest(draw_box("3. Choose an analysis", padding_lines=0)):
    action = prompts.list_input(
      "Which an analysis?",
      choices=[
        ("Exit", "exit"),
        ("N-grams analysis", "ngrams"),
      ],
    )

  if action == "exit" or action is None:
    return

  if action == "ngrams":
    print("Coming soon")
    wait_for_key(True)


def present_column_type(type: ExtensionDtype):
  if str(type) == "object":
    return "string"
  return str(type)
