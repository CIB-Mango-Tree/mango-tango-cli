import os
from typing import Optional

import polars as pl
from pydantic import BaseModel

from analyzer_interface import (
    AnalyzerInterface,
    AnalyzerOutput,
    SecondaryAnalyzerInterface,
)
from analyzer_interface.suite import AnalyzerSuite
from storage import AnalysisModel, Storage, SupportedOutputExtension
from terminal_tools import (
    ProgressReporter,
    open_directory_explorer,
    prompts,
    wait_for_key,
)
from terminal_tools.inception import TerminalContext
from terminal_tools.progress import ProgressReporter


def export_outputs(
    context: TerminalContext,
    storage: Storage,
    suite: AnalyzerSuite,
    analysis: AnalysisModel,
    *,
    all=False,
):
    analyzer = suite.get_primary_analyzer(analysis.primary_analyzer_id)
    with context.nest("[Export Output]\n\n") as scope:
        outputs = sorted(
            get_all_exportable_outputs(storage, suite, analysis),
            key=lambda output: (
                "0" if output.secondary is None else "1_" + output.secondary.name,
                output.output.name,
            ),
        )

        if all:
            selected_outputs = outputs
        else:
            output_options = [(output.name, output) for output in outputs]
            if not output_options:
                print("There are no outputs for this analysis")
                wait_for_key(True)
                return

            selected_outputs: list[Output] = prompts.checkbox(
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
        export_outputs_sequence(storage, analysis, selected_outputs, format)


def export_outputs_sequence(
    storage: Storage,
    analysis: AnalysisModel,
    selected_outputs: list["Output"],
    format: SupportedOutputExtension,
):
    has_large_dfs = any(
        output.height(analysis, storage) > 50_000 for output in selected_outputs
    )

    export_chunk_size = None
    if has_large_dfs:
        settings = storage.get_settings()
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

                    storage.save_settings(export_chunk_size=export_chunk_size)
                    break

                if chunk_action == "whole":
                    storage.save_settings(export_chunk_size=False)
                    break

        else:
            export_chunk_size = settings.export_chunk_size or None

    print("Beginning export...")
    for selected_output in selected_outputs:
        with ProgressReporter(f"Exporting {selected_output.name}") as progress:
            export_progress = selected_output.export(
                analysis,
                storage,
                format=format,
                export_chunk_size=export_chunk_size,
            )
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
        open_directory_explorer(storage._get_project_exports_root_path(analysis))
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


def get_all_exportable_outputs(
    storage: Storage, suite: AnalyzerSuite, analysis: AnalysisModel
):
    analyzer = suite.get_primary_analyzer(analysis.primary_analyzer_id)
    return [
        *(
            Output(output=output, secondary=None)
            for output in analyzer.outputs
            if not output.internal
        ),
        *(
            Output(output=output, secondary=secondary)
            for secondary_id in storage.list_secondary_analyses(analysis)
            if (
                secondary := suite.get_secondary_analyzer_by_id(
                    analysis.primary_analyzer_id, secondary_id
                )
            )
            is not None
            for output in secondary.outputs
            if not output.internal
        ),
    ]


class Output(BaseModel):
    output: AnalyzerOutput
    secondary: Optional[SecondaryAnalyzerInterface]

    @property
    def name(self):
        return (
            f"{self.output.name} ({self.secondary.name if self.secondary else 'Base'})"
        )

    def export(
        self,
        analysis: AnalysisModel,
        storage: Storage,
        *,
        format: SupportedOutputExtension,
        export_chunk_size: Optional[int] = None,
    ):
        if self.secondary is None:
            return storage.export_project_primary_output(
                analysis,
                self.output.id,
                extension=format,
                spec=self.output,
                export_chunk_size=export_chunk_size,
            )
        else:
            return storage.export_project_secondary_output(
                analysis,
                self.secondary.id,
                self.output.id,
                extension=format,
                spec=self.output,
                export_chunk_size=export_chunk_size,
            )

    def height(self, analysis: AnalysisModel, storage: Storage):
        if self.secondary is None:
            return self.df_height(
                storage.get_primary_output_parquet_path(analysis, self.output.id)
            )
        else:
            return self.df_height(
                storage.get_secondary_output_parquet_path(
                    analysis, self.secondary.id, self.output.id
                )
            )

    @staticmethod
    def df_height(path: str) -> int:
        return pl.scan_parquet(path).select(pl.len()).collect().item()
