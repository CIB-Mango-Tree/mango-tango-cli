from colorama import Fore

from app import ProjectContext
from terminal_tools import draw_box, prompts, wait_for_key

from .analysis_main import analysis_main
from .context import ViewContext
from .new_analysis import new_analysis
from .select_analysis import select_analysis


def project_main(
    context: ViewContext,
    project: ProjectContext,
):
    terminal = context.terminal
    while True:
        with terminal.nest(
            draw_box(f"CIB Mango Tree/Dataset: {project.display_name}", padding_lines=0)
        ):
            action = prompts.list_input(
                "What would you like to do?",
                choices=[
                    ("New test", "new_analysis"),
                    ("View a previously run test", "select_analysis"),
                    ("Rename this dataset", "rename_project"),
                    ("Delete this dataset", "delete_project"),
                    ("(Back)", None),
                ],
            )

        if action is None:
            return

        if action == "new_analysis":
            analysis = new_analysis(context, project)
            if analysis is not None:
                analysis_main(context, analysis)
            continue

        if action == "select_analysis":
            analysis = select_analysis(project)
            if analysis is not None:
                analysis_main(context, analysis)
            continue

        if action == "delete_project":
            print(
                f"⚠️  Warning  ⚠️\n\n"
                f"This will permanently delete the imported dataset and all of its analyses, "
                f"including all of their exported outputs.\n\n"
                f"**Be sure to copy out any exports you want to keep before proceeding.**\n\n"
                f"The original file used to create the dataset will NOT be deleted.\n\n"
            )
            confirm = prompts.confirm(
                "Are you sure you want to delete this dataset?", default=False
            )
            if not confirm:
                print("Deletion canceled.")
                wait_for_key(True)
                continue

            safephrase = f"DELETE {project.display_name}"
            print(f"Type {Fore.RED}{safephrase}{Fore.RESET} to confirm deletion.")
            if prompts.text(f"(type the above to confirm)") != safephrase:
                print("Deletion canceled.")
                wait_for_key(True)
                continue

            project.delete()
            print("🔥 Dataset deleted.")
            wait_for_key(True)
            return

        if action == "rename_project":
            new_name = prompts.text("Enter a new name for this dataset")
            if not new_name:
                print("Renaming canceled.")
                wait_for_key(True)
                continue

            project.rename(new_name)
            print("🔥 Dataset renamed.")
            wait_for_key(True)
            continue
