from colorama import Fore

from analyzer_interface.suite import AnalyzerSuite
from storage import AnalysisModel, Storage
from terminal_tools import draw_box, open_directory_explorer, prompts, wait_for_key
from terminal_tools.inception import TerminalContext

from .analysis_web_server import analysis_web_server
from .export_outputs import export_outputs


def analysis_main(
    context: TerminalContext,
    storage: Storage,
    suite: AnalyzerSuite,
    analysis: AnalysisModel,
):
    while True:
        analyzer = suite.get_primary_analyzer(analysis.primary_analyzer_id)
        has_web_server = suite.find_web_presenters(analyzer)

        with context.nest(
            draw_box(f"Analysis: {analysis.display_name}", padding_lines=0)
        ):
            action = prompts.list_input(
                "What would you like to do?",
                choices=[
                    ("Open output directory", "open_output_dir"),
                    ("Export outputs", "export_output"),
                    *([("Launch Web Server", "web_server")] if has_web_server else []),
                    ("Rename", "rename"),
                    ("Delete", "delete"),
                    ("(Back)", None),
                ],
            )

        if action is None:
            return

        if action == "open_output_dir":
            print("Starting file explorer")
            open_directory_explorer(storage._get_project_exports_root_path(analysis))
            wait_for_key(True)
            continue

        if action == "export_output":
            export_outputs(context, storage, suite, analysis)
            continue

        if action == "web_server":
            analysis_web_server(context, storage, suite, analysis)
            continue

        if action == "rename":
            new_name = prompts.text("Enter new name", default=analysis.display_name)
            if new_name is None:
                print("Rename canceled")
                wait_for_key(True)
                continue

            analysis.display_name = new_name
            storage.save_analysis(analysis)
            print("Analysis renamed")
            wait_for_key(True)
            continue

        if action == "delete":
            print(
                f"⚠️  Warning  ⚠️\n\n"
                f"This will permanently delete the analysis and all its outputs, "
                f"including the default export directory. "
                f"**Be sure to copy out any exports you want to keep before proceeding.**\n\n"
                f"The web dashboad will also no longer be accessible.\n\n"
            )
            confirm = prompts.confirm("Are you sure you want to delete this analysis?")
            if not confirm:
                print("Deletion canceled.")
                wait_for_key(True)
                continue

            safephrase = f"DELETE {analysis.display_name}"
            print(f"Type {Fore.RED}{safephrase}{Fore.RESET} to confirm deletion.")
            if prompts.text(f"(type the above to confirm)") != safephrase:
                print("Deletion canceled.")
                wait_for_key(True)
                continue

            storage.delete_analysis(analysis)
            print("🔥 Analysis deleted.")
            wait_for_key(True)
            return
