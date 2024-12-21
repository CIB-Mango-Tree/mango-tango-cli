from functools import cached_property
from typing import Literal, Optional

from pydantic import BaseModel

from analyzer_interface import AnalyzerOutput, SecondaryAnalyzerInterface
from storage import SupportedOutputExtension

from .analysis_context import AnalysisContext
from .app_context import AppContext
from .utils import parquet_row_count


class AnalysisOutputContext(BaseModel):
    app_context: AppContext
    analysis_context: AnalysisContext
    secondary_spec: Optional[SecondaryAnalyzerInterface]
    output_spec: AnalyzerOutput

    @property
    def descriptive_qualified_name(self):
        return f"{self.output_spec.name} ({self.secondary_spec.name if self.secondary_spec else 'Base'})"

    def export(
        self,
        *,
        format: SupportedOutputExtension,
        chunk_size_override: Optional[int | Literal[False]] = None,
    ):
        export_chunk_size = (
            self.app_context.settings.export_chunk_size
            if chunk_size_override is None
            else chunk_size_override
        ) or None
        if self.secondary_spec is None:
            return self.app_context.storage.export_project_primary_output(
                self.analysis_context.model,
                self.output_spec.id,
                extension=format,
                spec=self.output_spec,
                export_chunk_size=export_chunk_size,
            )
        else:
            return self.app_context.storage.export_project_secondary_output(
                self.analysis_context.model,
                self.secondary_spec.id,
                self.output_spec.id,
                extension=format,
                spec=self.output_spec,
                export_chunk_size=export_chunk_size,
            )

    @cached_property
    def num_rows(
        self,
    ):
        if self.secondary_spec is None:
            return parquet_row_count(
                self.app_context.storage.get_primary_output_parquet_path(
                    self.analysis_context.model, self.output_spec.id
                )
            )
        else:
            return parquet_row_count(
                self.app_context.storage.get_secondary_output_parquet_path(
                    self.analysis_context.model,
                    self.secondary_spec.id,
                    self.output_spec.id,
                )
            )
