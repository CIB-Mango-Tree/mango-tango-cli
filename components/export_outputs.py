import os

from app import AnalysisContext, AnalysisOutputContext
from storage import SupportedOutputExtension
from terminal_tools import (
    ProgressReporter,
    open_directory_explorer,
    prompts,
    wait_for_key,
)
from terminal_tools.progress import ProgressReporter

from .context import ViewContext


def export_outputs(context: ViewContext, analysis: AnalysisContext):
    terminal = context.terminal
    with terminal.nest("[Export Output]\n\n") as scope:
        outputs = sorted(
            analysis.get_all_exportable_outputs(),
            key=lambda output: (
                (
                    "0"
                    if output.secondary_spec is None
                    else "1_" + output.secondary_spec.name
                ),
                output.descriptive_qualified_name,
            ),
        )

        output_options = [
            (output.descriptive_qualified_name, output) for output in outputs
        ]
        if not output_options:
            print("There are no outputs for this analysis")
            wait_for_key(True)
            return

        selected_outputs: list[AnalysisOutputContext] = prompts.checkbox(
            "Choose output(s) to export", choices=output_options
        )

        if not selected_outputs:
            print("Export cancelled")
            wait_for_key(True)
            return

        scope.refresh()
        format = export_format_prompt()
        if format is None:
            print("Export cancelled")
            wait_for_key(True)
            return

        scope.refresh()
        export_outputs_sequence(context, analysis, selected_outputs, format)


def export_outputs_sequence(
    context: ViewContext,
    analysis: AnalysisContext,
    selected_outputs: list[AnalysisOutputContext],
    format: SupportedOutputExtension,
):
    has_large_dfs = any(output.num_rows > 50_000 for output in selected_outputs)

    settings = context.app.context.settings
    if has_large_dfs:
        if settings.export_chunk_size is None:
            print(f"Some of your exports will have more than 50,000 rows.")
            print(f"Let's take a moment to consider how you would like to proceed.")

            while True:
                chunk_action = prompts.list_input(
                    "How would you like to handle large files?",
                    choices=[
                        ("Break them into chunks", "chunk"),
                        ("Export in a single file", "whole"),
                    ],
                )
                if chunk_action is None:
                    print("Export cancelled")
                    wait_for_key(True)
                    return

                if chunk_action == "chunk":
                    export_chunk_size = prompts.int_input(
                        "How many rows should each chunk contain?",
                        default=50_000,
                        min=100,
                    )
                    if export_chunk_size is None:
                        continue
                    settings.set_export_chunk_size(export_chunk_size)
                    break

                if chunk_action == "whole":
                    settings.set_export_chunk_size(False)
                    break

    print("Beginning export...")
    for selected_output in selected_outputs:
        with ProgressReporter(
            f"Exporting {selected_output.descriptive_qualified_name}"
        ) as progress:
            export_progress = selected_output.export(format=format)
            try:
                while True:
                    progress.update(next(export_progress))
            except StopIteration as e:
                exported_path = e.value
                progress.finish("as " + os.path.basename(exported_path))

    print("")
    print("Export complete!")
    if prompts.confirm(
        "Would you like to open the containing directory?", default=True
    ):
        open_directory_explorer(analysis.export_root_path)
        print("Directory opened")
    else:
        print("All done!")

    wait_for_key(True)


def export_format_prompt():
    return prompts.list_input(
        "Choose an export format",
        choices=[
            ("CSV", "csv"),
            ("Excel", "xlsx"),
            ("JSON", "json"),
            ("(Back)", None),
        ],
    )
