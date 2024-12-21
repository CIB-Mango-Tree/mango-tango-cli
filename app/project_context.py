from functools import cached_property

import polars as pl
from pydantic import BaseModel

from analyzer_interface import UserInputColumn as BaseUserInputColumn
from preprocessing.series_semantic import SeriesSemantic, infer_series_semantic
from storage import AnalysisModel, ProjectModel

from .app_context import AppContext


class ProjectContext(BaseModel):
    model: ProjectModel
    app_context: AppContext
    is_deleted: bool = False

    @property
    def display_name(self):
        return self.model.display_name

    @property
    def id(self):
        return self.model.id

    def rename(self, new_name: str):
        self.app_context.storage.rename_project(self.id, new_name)
        self.model.display_name = new_name

    def delete(self):
        self.app_context.storage.delete_project(self.id)
        self.is_deleted = True

    def create_analysis(self, primary_analyzer_id: str, column_mapping: dict[str, str]):
        assert not self.is_deleted, "Project is deleted"

        analyzer = self.app_context.suite.get_primary_analyzer(primary_analyzer_id)
        assert analyzer, f"Analyzer `{primary_analyzer_id}` not found"

        analysis_model = self.app_context.storage.init_analysis(
            self.id, analyzer.name, primary_analyzer_id, column_mapping
        )
        return self._create_analysis_context(analysis_model)

    def list_analyses(self):
        return [
            self._create_analysis_context(analysis_model)
            for analysis_model in self.app_context.storage.list_project_analyses(
                self.id
            )
        ]

    def _create_analysis_context(self, analysis_model: AnalysisModel):
        from .analysis_context import AnalysisContext

        return AnalysisContext(
            model=analysis_model, project_context=self, app_context=self.app_context
        )

    @cached_property
    def preview_data(self):
        return self.app_context.storage.load_project_input(self.id, n_records=100)

    @cached_property
    def data_row_count(self):
        return self.app_context.storage.get_project_input_stats(self.id).num_rows

    @cached_property
    def columns(self):
        return _get_columns_with_semantic(self.preview_data)


def _get_columns_with_semantic(df: pl.DataFrame):
    return [
        UserInputColumn(
            name=col, data_type=semantic.data_type, semantic=semantic, data=df[col]
        )
        for col in df.columns
        if (semantic := infer_series_semantic(df[col])) is not None
    ]


class UserInputColumn(BaseUserInputColumn):
    semantic: SeriesSemantic
    data: pl.Series

    def head(self, n: int = 10):
        return UserInputColumn(
            name=self.name,
            data_type=self.data_type,
            semantic=self.semantic,
            data=self.data.head(n),
        )

    def apply_semantic_transform(self):
        return self.semantic.try_convert(self.data)

    class Config:
        arbitrary_types_allowed = True
