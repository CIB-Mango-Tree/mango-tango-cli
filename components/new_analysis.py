import tempfile
from traceback import format_exc
from typing import Optional

import polars as pl

from analyzer_interface import (
    AnalyzerInterface,
    InputColumn,
    UserInputColumn,
    column_automap,
    get_data_type_compatibility_score,
)
from analyzer_interface.suite import AnalyzerSuite
from context import (
    InputColumnProvider,
    PrimaryAnalyzerContext,
    SecondaryAnalyzerContext,
)
from storage import Project, Storage
from terminal_tools import draw_box, print_ascii_table, prompts, wait_for_key
from terminal_tools.inception import TerminalContext

from .export_outputs import (
    export_format_prompt,
    export_outputs_sequence,
    get_all_exportable_outputs,
)
from .utils import get_user_columns


def new_analysis(
    context: TerminalContext, storage: Storage, suite: AnalyzerSuite, project: Project
):
    with context.nest(draw_box("Choose a test", padding_lines=0)):
        analyzer: Optional[AnalyzerInterface] = prompts.list_input(
            "Which test?",
            choices=[
                ("(Back)", None),
                *(
                    (f"{analyzer.name} ({analyzer.short_description})", analyzer)
                    for analyzer in suite.primary_anlyzers
                ),
            ],
        )

    if analyzer is None:
        return

    with context.nest(draw_box(analyzer.name, padding_lines=0)):
        with context.nest("◆◆ About this test ◆◆"):

            print("")
            print(analyzer.long_description or analyzer.short_description)
            print("")
            print("◆◆ Required Input ◆◆")
            print("The test requires these columns in the input data:")
            print("")
            for index, input_column in enumerate(analyzer.input.columns):
                print(
                    f"[{index + 1}] {input_column.human_readable_name_or_fallback()}"
                    f" ({input_column.data_type})"
                )
                print(input_column.description or "")
                print("")

            sample_project_df = storage.load_project_input(project.id, n_records=100)
            user_columns = get_user_columns(sample_project_df)
            user_columns_by_name = {
                user_column.name: user_column for user_column in user_columns
            }
            draft_column_mapping = column_automap(user_columns, analyzer.input.columns)
            unmapped_columns = list(
                input_column
                for input_column in analyzer.input.columns
                if draft_column_mapping.get(input_column.name) is None
            )

            if len(unmapped_columns) > 0:
                print("Your dataset does NOT have all the types of columns required.")
                print("These columns cannot be satisfied:")
                for input_column in unmapped_columns:
                    print(
                        f"- {input_column.human_readable_name_or_fallback()
                     } ({input_column.data_type})"
                    )

                print("")
                print("The analysis will now exit.")
                print("")
                wait_for_key(True)
                return
            else:
                print(
                    "You will now choose which columns in your dataset to use for the test."
                )
                wait_for_key(True)

        final_column_mapping = draft_column_mapping
        while True:
            with context.nest("Column mapping") as column_mapping_scope:
                print_ascii_table(
                    rows=[
                        [
                            input_column.human_readable_name_or_fallback(),
                            '"' + draft_column_mapping.get(input_column.name) + '"',
                        ]
                        for input_column in analyzer.input.columns
                    ],
                    header=["Test's Input Column", "← Your Dataset's Column"],
                )

                sample_input_df = pl.DataFrame(
                    {
                        input_col.human_readable_name_or_fallback(): user_columns_by_name.get(
                            final_column_mapping.get(input_col.name)
                        )
                        .head(5)
                        .apply_semantic_transform()
                        for input_col in analyzer.input.columns
                    }
                )
                print("Your test data would look like this:")
                print(sample_input_df)

                mapping_ok = prompts.confirm(
                    "Are you happy with this mapping?",
                    default=False,
                    cancel_fallback=None,
                )

                if mapping_ok is None:
                    print("Canceled")
                    wait_for_key(True)
                    return

                if mapping_ok:
                    break

                column_mapping_scope.refresh()
                selected_analyzer_column: Optional[InputColumn] = prompts.list_input(
                    "Choose the input column to re-assign",
                    choices=[
                        (
                            input_column.human_readable_name_or_fallback()
                            + (
                                ": " + input_column.description
                                if input_column.description
                                else ""
                            ),
                            input_column,
                        )
                        for input_column in analyzer.input.columns
                    ],
                )

                if selected_analyzer_column is None:
                    break

                column_mapping_scope.refresh()
                print("You are re-assigning data for this test input column:")
                print(
                    "["
                    + selected_analyzer_column.human_readable_name_or_fallback()
                    + "]"
                )
                if selected_analyzer_column.description:
                    print("")
                    print("Explanation: " + selected_analyzer_column.description)
                print("")
                print(
                    f"The test requires data type [{
              selected_analyzer_column.data_type}] for this column."
                )
                print("")

                selected_user_column: Optional[UserInputColumn] = prompts.list_input(
                    "Choose your dataset's column to use",
                    choices=[
                        (
                            '"'
                            + user_column.name
                            + '" ['
                            + user_column.data_type
                            + "]",
                            user_column,
                        )
                        for user_column in user_columns
                        if get_data_type_compatibility_score(
                            selected_analyzer_column.data_type, user_column.data_type
                        )
                        is not None
                    ],
                )

                if selected_user_column is not None:
                    draft_column_mapping[selected_analyzer_column.name] = (
                        selected_user_column.name
                    )

        analysis = storage.init_analysis(
            project.id,
            analyzer.name,
            analyzer.id,
            final_column_mapping,
        )

        with context.nest("Analysis") as run_scope:
            is_export_started = False
            try:
                print("Starting base analysis for the test...")
                with tempfile.TemporaryDirectory() as temp_dir:
                    analyzer_context = PrimaryAnalyzerContext(
                        analysis=analysis,
                        analyzer=analyzer,
                        store=storage,
                        temp_dir=temp_dir,
                        input_columns={
                            analyzer_column_name: InputColumnProvider(
                                user_column_name=user_column_name,
                                semantic=user_columns_by_name[
                                    user_column_name
                                ].semantic,
                            )
                            for analyzer_column_name, user_column_name in final_column_mapping.items()
                        },
                    )
                    analyzer_context.prepare()
                    analyzer.entry_point(analyzer_context)

                for secondary in suite.find_toposorted_secondary_analyzers(analyzer):
                    run_scope.refresh()
                    print("Running post-analysis: ", secondary.name)

                    with tempfile.TemporaryDirectory() as temp_dir:
                        secondary_context = SecondaryAnalyzerContext(
                            analysis=analysis,
                            secondary_analyzer=secondary,
                            temp_dir=temp_dir,
                            store=storage,
                        )
                        secondary_context.prepare()
                        secondary.entry_point(secondary_context)

                analysis.is_draft = False
                storage.save_analysis(analysis)

                run_scope.refresh()
                print("The test is complete.")
                print("")

                print(f"The analysis is named '{analysis.display_name}'.")
                print(
                    "You can rename it now if you wish. Or just hit enter to continue."
                )
                analysis.display_name = (
                    prompts.text("Analysis name", default=analyzer.name) or ""
                ).strip() or analysis.display_name
                storage.save_analysis(analysis)

                print("")

                outputs = get_all_exportable_outputs(storage, suite, analysis)
                print("You now have the option to export the following outputs:")
                for output in outputs:
                    print("- " + output.name)
                print("")

                export_format = export_format_prompt()
                if export_format is None:
                    print(
                        "No problem. You can also export outputs later from the analysis menu."
                    )
                    wait_for_key(True)
                else:
                    is_export_started = True
                    export_outputs_sequence(storage, analysis, outputs, export_format)

                return analysis

            except KeyboardInterrupt:
                if is_export_started:
                    print(
                        "The export was interrupted -- the outputs may be incomplete."
                    )
                    print("You can re-attempt the export from the menu.")
                else:
                    print("The test run was canceled")

                wait_for_key(True)
                return None

            except Exception as e:
                traceback = format_exc()
                run_scope.refresh()
                print("An error occurred during the analysis:")
                print(e)
                print("")

                if prompts.confirm(
                    "Would you like to see the full error traceback?", default=False
                ):
                    print(traceback)

                print("")
                print(
                    "Help us improve this tool by reporting this error on our GitHub repository:"
                )
                print("https://github.com/CIB-Mango-Tree/mango-tango-cli")
                print("")
                wait_for_key(True)

                return None

            finally:
                if analysis.is_draft:
                    storage.delete_analysis(analysis)
