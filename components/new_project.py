import csv
import os

import polars as pl

from storage import Storage
from terminal_tools import draw_box, prompts, wait_for_key
from terminal_tools.inception import TerminalContext

from .utils import ProjectInstance, input_preview


def new_project(context: TerminalContext, storage: Storage):
  with context.nest(draw_box("1. Data Source", padding_lines=0)):
    print("Select a file for your dataset")
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

          df = pl.read_csv(
            selected_file,
            separator=dialect.delimiter,
            quote_char=dialect.quotechar,
            ignore_errors=True,
            has_header=True,
            truncate_ragged_lines=True,
          )
      except Exception as e:
        print(f"Error reading CSV file: {e}")
        wait_for_key(True)
        return

    else:
      print(
        f"Unsupported file type: {
          file_extension or '(file with no extension)'}"
      )
      wait_for_key(True)
      return

  with context.nest(draw_box("2. Data preview", padding_lines=0)):
    input_preview(df)
    wait_for_key(True)

  with context.nest(draw_box("3. Naming", padding_lines=0)):
    print("Rename the dataset if you wish. This is how the dataset will appear when you try to load it again.")
    suggested_project_name = os.path.splitext(
      os.path.basename(selected_file))[0]
    project_name = prompts.text(
      "Name", default=suggested_project_name
    )

    project = storage.init_project(display_name=project_name, input=df)
    print("Dataset successfully imported!")
    wait_for_key(True)
    return ProjectInstance(
      id=project.id,
      display_name=project.display_name,
      input=df
    )
