import os
from functools import cached_property

import polars as pl
from dash import Dash
from pydantic import BaseModel

from analyzer_interface import (
    AnalyzerInterface,
    SecondaryAnalyzerInterface,
    WebPresenterInterface,
)
from analyzer_interface.context import AssetsReader, InputTableReader
from analyzer_interface.context import (
    PrimaryAnalyzerContext as BasePrimaryAnalyzerContext,
)
from analyzer_interface.context import (
    SecondaryAnalyzerContext as BaseSecondaryAnalyzerContext,
)
from analyzer_interface.context import TableReader, TableWriter
from analyzer_interface.context import WebPresenterContext as BaseWebPresenterContext
from preprocessing.series_semantic import SeriesSemantic
from storage import AnalysisModel, Storage


class PrimaryAnalyzerContext(BasePrimaryAnalyzerContext):
    analysis: AnalysisModel
    analyzer: AnalyzerInterface
    store: Storage
    input_columns: dict[str, "InputColumnProvider"]

    class Config:
        arbitrary_types_allowed = True

    def input(self) -> InputTableReader:
        return PrimaryAnalyzerInputTableReader(
            project_id=self.analysis.project_id,
            analyzer=self.analyzer,
            store=self.store,
            input_columns=self.input_columns,
        )

    def output(self, output_id: str) -> TableWriter:
        return PrimaryAnalyzerOutputWriter(
            analysis=self.analysis,
            output_id=output_id,
            store=self.store,
        )

    def prepare(self):
        os.makedirs(
            self.store._get_project_primary_output_root_path(self.analysis),
            exist_ok=True,
        )


class InputColumnProvider(BaseModel):
    user_column_name: str
    semantic: SeriesSemantic


class PrimaryAnalyzerOutputWriter(TableWriter, BaseModel):
    analysis: AnalysisModel
    output_id: str
    store: Storage

    class Config:
        arbitrary_types_allowed = True

    @cached_property
    def parquet_path(self):
        return self.store.get_primary_output_parquet_path(self.analysis, self.output_id)


class PrimaryAnalyzerInputTableReader(InputTableReader, BaseModel):
    project_id: str
    analyzer: AnalyzerInterface
    store: Storage
    input_columns: dict[str, InputColumnProvider]

    class Config:
        arbitrary_types_allowed = True

    @cached_property
    def parquet_path(self):
        return self.store._get_project_input_path(self.project_id)

    def preprocess(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.select(
            [
                pl.col(provider.user_column_name)
                .map_batches(provider.semantic.try_convert)
                .alias(input_column_name)
                for input_column_name, provider in self.input_columns.items()
            ]
        )


class SecondaryAnalyzerContext(BaseSecondaryAnalyzerContext):
    analysis: AnalysisModel
    secondary_analyzer: SecondaryAnalyzerInterface
    store: Storage
    temp_dir: str

    class Config:
        arbitrary_types_allowed = True

    @cached_property
    def base(self) -> AssetsReader:
        return PrimaryAnalyzerOutputReaderGroupContext(
            analysis=self.analysis, store=self.store
        )

    def dependency(self, interface: SecondaryAnalyzerInterface) -> AssetsReader:
        return SecondaryAnalyzerOutputReaderGroupContext(
            analysis=self.analysis, secondary_analyzer_id=interface.id, store=self.store
        )

    def temp_dir(self) -> str:
        return self.temp_dir

    def output(self, output_id: str) -> TableWriter:
        return SecondaryAnalyzerOutputWriter(
            analysis=self.analysis,
            secondary_analyzer_id=self.secondary_analyzer.id,
            output_id=output_id,
            store=self.store,
        )

    def prepare(self):
        os.makedirs(
            self.store._get_project_secondary_output_root_path(
                self.analysis, self.secondary_analyzer.id
            ),
            exist_ok=True,
        )


class WebPresenterContext(BaseWebPresenterContext):
    analysis: AnalysisModel
    web_presenter: WebPresenterInterface
    store: Storage
    dash_app: Dash

    class Config:
        arbitrary_types_allowed = True

    @cached_property
    def base(self) -> AssetsReader:
        return PrimaryAnalyzerOutputReaderGroupContext(
            analysis=self.analysis, store=self.store
        )

    def dependency(self, interface: SecondaryAnalyzerInterface) -> AssetsReader:
        return SecondaryAnalyzerOutputReaderGroupContext(
            analysis=self.analysis,
            secondary_analyzer_id=interface.id,
            store=self.store,
        )

    @cached_property
    def state_dir(self) -> str:
        return self.store._get_web_presenter_state_path(
            self.analysis.project_id, self.web_presenter.id
        )


class PrimaryAnalyzerOutputReaderGroupContext(AssetsReader, BaseModel):
    analysis: AnalysisModel
    store: Storage

    class Config:
        arbitrary_types_allowed = True

    def table(self, output_id: str) -> TableReader:
        return PrimaryAnalyzerOutputTableReader(
            analysis=self.analysis, output_id=output_id, store=self.store
        )


class PrimaryAnalyzerOutputTableReader(TableReader, BaseModel):
    analysis: AnalysisModel
    output_id: str
    store: Storage

    class Config:
        arbitrary_types_allowed = True

    @cached_property
    def parquet_path(self):
        return self.store.get_primary_output_parquet_path(self.analysis, self.output_id)


class SecondaryAnalyzerOutputReaderGroupContext(AssetsReader, BaseModel):
    analysis: AnalysisModel
    secondary_analyzer_id: str
    store: Storage

    class Config:
        arbitrary_types_allowed = True

    def table(self, output_id: str) -> TableReader:
        return SecondaryAnalyzerOutputTableReader(
            analysis=self.analysis,
            secondary_analyzer_id=self.secondary_analyzer_id,
            output_id=output_id,
            store=self.store,
        )


class SecondaryAnalyzerOutputTableReader(TableReader, BaseModel):
    analysis: AnalysisModel
    secondary_analyzer_id: str
    output_id: str
    store: Storage

    class Config:
        arbitrary_types_allowed = True

    @cached_property
    def parquet_path(self):
        return self.store.get_secondary_output_parquet_path(
            self.analysis, self.secondary_analyzer_id, self.output_id
        )


class SecondaryAnalyzerOutputWriter(TableWriter, BaseModel):
    analysis: AnalysisModel
    secondary_analyzer_id: str
    output_id: str
    store: Storage

    class Config:
        arbitrary_types_allowed = True

    @cached_property
    def parquet_path(self):
        return self.store.get_secondary_output_parquet_path(
            self.analysis, self.secondary_analyzer_id, self.output_id
        )
