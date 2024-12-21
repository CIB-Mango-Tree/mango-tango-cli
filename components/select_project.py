from typing import Optional

from app import ProjectContext
from terminal_tools import draw_box, print_ascii_table, prompts, wait_for_key

from .context import ViewContext


def select_project(ctx: ViewContext):
    terminal = ctx.terminal
    app = ctx.app

    while True:
        with terminal.nest(draw_box("Choose a project", padding_lines=0)):
            projects = app.list_projects()
            if not projects:
                print("There are no previously created projects.")
                wait_for_key(True)
                return None

            project: Optional[ProjectContext] = prompts.list_input(
                "Which project?",
                choices=[(project.display_name, project) for project in projects],
            )

            if project is None:
                return None

        with terminal.nest(
            draw_box(f"Project: {project.display_name}", padding_lines=0)
        ):
            df = project.preview_data
            print_ascii_table(
                [
                    [preview_value(cell) for cell in row]
                    for row in df.head(10).iter_rows()
                ],
                header=df.columns,
            )
            print(f"(Total {project.data_row_count} rows)")
            print("Inferred column semantics:")
            print_ascii_table(
                rows=[
                    [col.name, col.semantic.semantic_name] for col in project.columns
                ],
                header=["Column", "Semantic"],
            )

            confirm_load = prompts.confirm("Load this project?", default=True)
            if confirm_load:
                return project


def preview_value(value):
    if isinstance(value, str):
        if len(value) > 20:
            return value[:20] + "..."
        return value
    if value is None:
        return "(N/A)"
    return value
