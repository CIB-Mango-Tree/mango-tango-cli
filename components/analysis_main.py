from colorama import Fore

from app import AnalysisContext
from terminal_tools import draw_box, open_directory_explorer, prompts, wait_for_key

from .context import ViewContext
from .export_outputs import export_outputs


def analysis_main(context: ViewContext, analysis: AnalysisContext):
    terminal = context.terminal
    while True:
        has_web_server = len(analysis.web_presenters) > 0
        is_draft = analysis.is_draft

        with terminal.nest(
            draw_box(f"Analysis: {analysis.display_name}", padding_lines=0)
        ):
            if is_draft:
                print("⚠️  This analysis didn't complete successfully.  ⚠️")

            action = prompts.list_input(
                "What would you like to do?",
                choices=[
                    *(
                        [
                            ("Open output directory", "open_output_dir"),
                            ("Export outputs", "export_output"),
                        ]
                        if not is_draft
                        else []
                    ),
                    *(
                        [("Launch Web Server", "web_server")]
                        if (not is_draft) and has_web_server
                        else []
                    ),
                    ("Rename", "rename"),
                    ("Delete", "delete"),
                    ("(Back)", None),
                ],
            )

        if action is None:
            return

        if action == "open_output_dir":
            print("Starting file explorer")
            open_directory_explorer(analysis.export_root_path)
            wait_for_key(True)
            continue

        if action == "export_output":
            export_outputs(context, analysis)
            continue

        if action == "web_server":
            server = analysis.web_server()
            print("Web server will run at http://localhost:8050/")
            print("Stop it with Ctrl+C")
            try:
                server.start()
            except KeyboardInterrupt:
                pass
            print("Web server stopped")
            wait_for_key(True)
            continue

        if action == "rename":
            new_name = prompts.text("Enter new name", default=analysis.display_name)
            if new_name is None:
                print("Rename canceled")
                wait_for_key(True)
                continue

            analysis.rename(new_name)
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

            analysis.delete()
            print("🔥 Analysis deleted.")
            wait_for_key(True)
            return
