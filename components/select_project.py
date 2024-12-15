from storage import Project, Storage
from terminal_tools import draw_box, prompts, wait_for_key
from terminal_tools.inception import TerminalContext

from .utils import input_preview


def select_project(context: TerminalContext, storage: Storage):
    while True:
        with context.nest(draw_box("Choose a project", padding_lines=0)):
            projects = storage.list_projects()
            if not projects:
                print("There are no previously created projects.")
                wait_for_key(True)
                return None

            project = prompts.list_input(
                "Which project?",
                choices=[(project.display_name, project) for project in projects],
            )

            if project is None:
                return None

        with context.nest(
            draw_box(f"Project: {project.display_name}", padding_lines=0)
        ):
            df = storage.load_project_input(project.id, n_records=100)
            table_stats = storage.get_project_input_stats(project.id)
            input_preview(df, table_stats)
            confirm_load = prompts.confirm("Load this project?", default=True)
            if confirm_load:
                return Project(id=project.id, display_name=project.display_name)
