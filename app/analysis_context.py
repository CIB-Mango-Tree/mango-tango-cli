from functools import cached_property
from tempfile import TemporaryDirectory
from typing import Literal

from pydantic import BaseModel

from analyzer_interface import AnalyzerDeclaration, SecondaryAnalyzerDeclaration
from context import (
    InputColumnProvider,
    PrimaryAnalyzerContext,
    SecondaryAnalyzerContext,
)
from storage import AnalysisModel

from .app_context import AppContext
from .project_context import ProjectContext


class AnalysisRunProgressEvent(BaseModel):
    analyzer: AnalyzerDeclaration | SecondaryAnalyzerDeclaration
    event: Literal["start", "finish"]


class AnalysisContext(BaseModel):
    app_context: AppContext
    project_context: ProjectContext
    model: AnalysisModel
    is_deleted: bool = False

    @property
    def display_name(self):
        return self.model.display_name

    @property
    def id(self):
        return self.model.id

    @property
    def analyzer_id(self):
        return self.model.primary_analyzer_id

    @property
    def analyzer_spec(self):
        analyzer = self.app_context.suite.get_primary_analyzer(self.analyzer_id)
        assert analyzer, f"Analyzer `{self.analyzer_id}` not found"
        return analyzer

    @property
    def column_mapping(self):
        return self.model.column_mapping

    @property
    def create_time(self):
        return self.model.create_time()

    @property
    def is_draft(self):
        return self.model.is_draft

    @cached_property
    def web_presenters(self):
        return self.app_context.suite.find_web_presenters(self.analyzer_spec)

    def web_server(self):
        from .analysis_webserver_context import AnalysisWebServerContext

        return AnalysisWebServerContext(
            app_context=self.app_context, analysis_context=self
        )

    def rename(self, new_name: str):
        self.model.display_name = new_name
        self.app_context.storage.save_analysis(self.model)

    def delete(self):
        self.is_deleted = True
        self.app_context.storage.delete_analysis(self.model)

    def run(self):
        assert not self.is_deleted, "Analysis is deleted"
        secondary_analyzers = (
            self.app_context.suite.find_toposorted_secondary_analyzers(
                self.analyzer_spec
            )
        )

        with TemporaryDirectory() as temp_dir:
            yield AnalysisRunProgressEvent(analyzer=self.analyzer_spec, event="start")
            user_columns_by_name = {
                user_column.name: user_column
                for user_column in self.project_context.columns
            }
            analyzer_context = PrimaryAnalyzerContext(
                analysis=self.model,
                analyzer=self.analyzer_spec,
                store=self.app_context.storage,
                temp_dir=temp_dir,
                input_columns={
                    analyzer_column_name: InputColumnProvider(
                        user_column_name=user_column_name,
                        semantic=user_columns_by_name[user_column_name].semantic,
                    )
                    for analyzer_column_name, user_column_name in self.column_mapping.items()
                },
            )
            analyzer_context.prepare()
            self.analyzer_spec.entry_point(analyzer_context)
            yield AnalysisRunProgressEvent(analyzer=self.analyzer_spec, event="finish")

        for secondary in secondary_analyzers:
            yield AnalysisRunProgressEvent(analyzer=secondary, event="start")
            with TemporaryDirectory() as temp_dir:
                analyzer_context = SecondaryAnalyzerContext(
                    analysis=self.model,
                    secondary_analyzer=secondary,
                    temp_dir=temp_dir,
                    store=self.app_context.storage,
                )
                analyzer_context.prepare()
                secondary.entry_point(analyzer_context)
            yield AnalysisRunProgressEvent(analyzer=secondary, event="finish")

        self.model.is_draft = False
        self.app_context.storage.save_analysis(self.model)

    @property
    def export_root_path(self):
        return self.app_context.storage._get_project_exports_root_path(self.model)

    def get_all_exportable_outputs(self):
        from .analysis_output_context import AnalysisOutputContext

        return [
            *(
                AnalysisOutputContext(
                    app_context=self.app_context,
                    analysis_context=self,
                    secondary_spec=None,
                    output_spec=output,
                )
                for output in self.analyzer_spec.outputs
                if not output.internal
            ),
            *(
                AnalysisOutputContext(
                    app_context=self.app_context,
                    analysis_context=self,
                    secondary_spec=secondary,
                    output_spec=output,
                )
                for secondary_id in self.app_context.storage.list_secondary_analyses(
                    self.model
                )
                if (
                    secondary := self.app_context.suite.get_secondary_analyzer_by_id(
                        self.analyzer_id, secondary_id
                    )
                )
                is not None
                for output in secondary.outputs
                if not output.internal
            ),
        ]
