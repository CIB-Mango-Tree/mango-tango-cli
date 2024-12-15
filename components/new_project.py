import os
import tempfile
from traceback import format_exc
from typing import Optional

from importing import Importer, ImporterSession, importers
from storage import Project, Storage
from terminal_tools import draw_box, prompts, wait_for_key
from terminal_tools.inception import Scope, TerminalContext


def new_project(context: TerminalContext, storage: Storage):
  with context.nest(draw_box("1. Data Source", padding_lines=0)):
    print("Select a file for your dataset")
    selected_file = prompts.file_selector(
      "Select a file", state=storage.file_selector_state
    )
    if selected_file is None:
      print("Canceled")
      return wait_for_key(True)

  with context.nest(draw_box("2. Import Options", padding_lines=0)) as scope:
    importer: Optional[ImporterSession] = importer_flow(
      selected_file, importers, scope)
    if importer is None:
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
      importer.import_as_parquet(temp_file.name)

    project = storage.init_project(
      display_name=project_name, input_temp_file=temp_file.name)

    print("Dataset successfully imported!")
    wait_for_key(True)
    return Project(
      id=project.id,
      display_name=project.display_name
    )


def importer_flow(
  input_file: str,
  importers: list[Importer[ImporterSession]],
  scope: Scope
):
  suggested_importer: Optional[Importer[ImporterSession]] = None
  for importer in importers:
    if importer.suggest(input_file):
      suggested_importer = importer
      break

  importer = suggested_importer
  is_first_time_importer_infer_error = True

  import_session = None
  import_preview = None
  import_session_error = None
  import_session_traceback = None
  while True:
    scope.refresh()
    print(f"Importing {input_file}")
    print("")

    if not import_session:
      if not importer:
        if not is_first_time_importer_infer_error:
          print("Could not figure out how to import this file.")
          is_first_time_importer_infer_error = False

        print("Please select a format to open the file as.")
        importer = prompts.list_input(
          "Select a format",
          choices=[(importer.name, importer) for importer in importers]
        )

      if importer is None:
        return None

    if not import_session:
      try:
        import_session = importer.init_session(input_file)
      except Exception as e:
        import_session_error = e
        import_session_traceback = format_exc()

    if import_session:
      import_preview = None
      import_session_error = None
      import_session_traceback = None
      try:
        import_preview = import_session.load_preview(1)
      except Exception as e:
        import_session_error = e
        import_session_traceback = format_exc()

    if import_session:
      if import_session_error:
        print("Could not load a preview of the data.")
        print(f"❌ Error:\n{indent_error(str(import_session_error))}")
        print(f"Importing as {importer.name} with these options:")
        import_session.print_config()
        print("")
      else:
        print(f"Importing as {importer.name} with these options:")
        import_session.print_config()
        print("")
        print("The data has these columns:")
        print("(Each column should be listed in its own line)")
        for column in import_preview.columns:
          print(f"  [{column}]")
        print("")
    else:
      print("Could not figure out how to import this file as {importer.name}.")

    import_action = prompts.list_input(
      "What would you like to do?",
      choices=[
        *([
            ("Import this file", "import"),
          ] if import_preview is not None else []),
        *([
            ("Modify the import options", "modify")
          ] if import_session else [
            ("Manually configure the import options", "manual")
          ]),
        *([
            ("View the import error", "view_error")
          ] if import_session_error else []),
        *([
          ("Open this file as a different format", "different_format")
          ] if len(importers) > 1 else []),
        ("Cancel", None)
      ]
    )

    if import_action is None:
      return None

    if import_action == "import":
      assert import_session is not None
      return import_session

    if import_action == "different_format":
      import_session = None
      importer = None
      continue

    if import_action == "manual":
      import_session = importer.manual_init_session(input_file)
      continue

    if import_action == "modify":
      assert import_session is not None

      def reset_screen_for_customize_session(session=import_session):
        scope.refresh()
        print(f"Importing {input_file}")
        print("")
        print(f"Importing as {importer.name} with these options:")
        session.print_config()

      import_session = importer.modify_session(
        input_file, import_session, reset_screen_for_customize_session
      )
      if import_session is None:
        continue

    if import_action == "view_error":
      assert import_session_error is not None
      assert import_session_traceback is not None
      print(f"Error:\n{indent_error(str(import_session_traceback))}")
      wait_for_key(True)
      continue


def indent_error(err: str):
  return "\n".join([" ! " + line for line in err.split("\n")]) + "\n"
