import os
import tempfile
from typing import Optional

from importing import IImporterPreload, Importer, importers
from storage import Project, Storage
from terminal_tools import draw_box, prompts, wait_for_key
from terminal_tools.inception import TerminalContext

from .utils import input_preview


def new_project(context: TerminalContext, storage: Storage):
  with context.nest(draw_box("1. Data Source", padding_lines=0)):
    print("Select a file for your dataset")
    selected_file = prompts.file_selector("Select a file")
    if selected_file is None:
      print("Canceled")
      return wait_for_key(True)

    print(f"Selected file: {selected_file}")
    import_spec: Optional[tuple[Importer, IImporterPreload]] = None
    import_errors: list[tuple[Importer, Exception]] = []

    for importer in importers:
      if not importer.sniff(selected_file):
        continue

      try:
        preload = importer.preload(selected_file, 100)
        import_spec = (importer, preload)
      except Exception as e:
        import_errors.append((importer, e))
        continue

      print(f"Input loaded as a {importer.name} file.")
      break

    if import_spec is None:
      print("We couldn't open this file. It is likely that the file is not supported.")
      if prompts.confirm("Would you like to see the errors?", default=False):
        for importer, e in import_errors:
          print(f"Error trying attempting to load as {importer.name}: {e}")
      return wait_for_key(True)

    importer, preload = import_spec

  with context.nest(draw_box("2. Data preview", padding_lines=0)):
    input_preview(preload.get_preview_dataframe())
    confirm_preview = prompts.confirm("Is this correct?", default=True)
    if not confirm_preview:
      print("Canceled")
      return wait_for_key(True)

  with context.nest(draw_box("3. Naming", padding_lines=0)):
    print("Rename the dataset if you wish. This is how the dataset will appear when you try to load it again.")
    suggested_project_name = os.path.splitext(
      os.path.basename(selected_file))[0]
    project_name = prompts.text(
      "Name", default=suggested_project_name
    )

  with context.nest(draw_box("4. Import", padding_lines=0)):
    print("Please wait as the dataset is imported...")
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
      importer.import_data(selected_file, temp_file.name, preload)

    project = storage.init_project(
      display_name=project_name, input_temp_file=temp_file.name)

    print("Dataset successfully imported!")
    wait_for_key(True)
    return Project(
      id=project.id,
      display_name=project.display_name
    )
