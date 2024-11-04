from functools import cached_property

import polars as pl
from dash import Dash
from pydantic import BaseModel

from analyzer_interface import (AnalyzerInterface, SecondaryAnalyzerInterface,
                                WebPresenterInterface)
from analyzer_interface.context import (PrimaryAnalyzerContext as BasePrimaryAnalyzerContext,
                                        SecondaryAnalyzerContext as BaseSecondaryAnalyzerContext,
                                        InputTableReader, AssetsReader,
                                        TableReader, TableWriter,
                                        WebPresenterContext as BaseWebPresenterContext)
from preprocessing.series_semantic import SeriesSemantic
from storage import Storage
import os


class PrimaryAnalyzerContext(BasePrimaryAnalyzerContext):
  project_id: str
  primary_analyzer: AnalyzerInterface
  store: Storage
  input_columns: dict[str, "InputColumnProvider"]

  class Config:
    arbitrary_types_allowed = True

  def input(self) -> InputTableReader:
    return PrimaryAnalyzerInputTableReader(
      project_id=self.project_id,
      analyzer=self.primary_analyzer,
      store=self.store,
      input_columns=self.input_columns
    )

  def output(self, output_id: str) -> TableWriter:
    return PrimaryAnalyzerOutputWriter(
      project_id=self.project_id,
      analyzer=self.primary_analyzer,
      output_id=output_id,
      store=self.store
    )

  def prepare(self):
    os.makedirs(
      self.store._get_project_primary_output_root_path(
        self.project_id, self.primary_analyzer.id
      ),
      exist_ok=True
    )


class InputColumnProvider(BaseModel):
  user_column_name: str
  semantic: SeriesSemantic


class PrimaryAnalyzerOutputWriter(TableWriter, BaseModel):
  project_id: str
  analyzer: AnalyzerInterface
  output_id: str
  store: Storage

  class Config:
    arbitrary_types_allowed = True

  @cached_property
  def parquet_path(self):
    return self.store.get_primary_output_parquet_path(self.project_id, self.analyzer.id, self.output_id)


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
    return df.select([
      pl.col(provider.user_column_name)
        .map_batches(provider.semantic.try_convert)
        .alias(input_column_name)
      for input_column_name, provider in self.input_columns.items()
    ])


class SecondaryAnalyzerContext(BaseSecondaryAnalyzerContext):
  project_id: str
  primary_analyzer: AnalyzerInterface
  secondary_analyzer: SecondaryAnalyzerInterface
  store: Storage
  temp_dir: str

  class Config:
    arbitrary_types_allowed = True

  @cached_property
  def base(self) -> AssetsReader:
    base_analyzer = self.secondary_analyzer.base_analyzer

    return PrimaryAnalyzerOutputReaderGroupContext(
      project_id=self.project_id,
      analyzer=base_analyzer,
      store=self.store
    )

  def dependency(self, interface: SecondaryAnalyzerInterface) -> AssetsReader:
    return SecondaryAnalyzerOutputReaderGroupContext(
      project_id=self.project_id,
      primary_analyzer=self.primary_analyzer,
      secondary_analyzer=interface,
      store=self.store
    )

  def temp_dir(self) -> str:
    return self.temp_dir

  def output(self, output_id: str) -> TableWriter:
    return SecondaryAnalyzerOutputWriter(
      project_id=self.project_id,
      primary_analyzer=self.primary_analyzer,
      secondary_analyzer=self.secondary_analyzer,
      output_id=output_id,
      store=self.store
    )

  def prepare(self):
    os.makedirs(
      self.store._get_project_secondary_output_root_path(
        self.project_id, self.primary_analyzer.id, self.secondary_analyzer.id,
      ),
      exist_ok=True
    )


class WebPresenterContext(BaseWebPresenterContext):
  project_id: str
  primary_analyzer: AnalyzerInterface
  web_presenter: WebPresenterInterface
  store: Storage
  dash_app: Dash

  class Config:
    arbitrary_types_allowed = True

  @cached_property
  def base(self) -> AssetsReader:
    return PrimaryAnalyzerOutputReaderGroupContext(
      project_id=self.project_id,
      analyzer=self.primary_analyzer,
      store=self.store
    )

  def dependency(self, interface: SecondaryAnalyzerInterface) -> AssetsReader:
    return SecondaryAnalyzerOutputReaderGroupContext(
      project_id=self.project_id,
      primary_analyzer=self.primary_analyzer,
      secondary_analyzer=interface,
      store=self.store
    )

  @cached_property
  def state_dir(self) -> str:
    return self.store.get_web_presenter_state_path(
      self.project_id, self.primary_analyzer.id, self.web_presenter.id
    )


class PrimaryAnalyzerOutputReaderGroupContext(AssetsReader, BaseModel):
  analyzer: AnalyzerInterface
  project_id: str
  store: Storage

  class Config:
    arbitrary_types_allowed = True

  def table(self, output_id: str) -> TableReader:
    return PrimaryAnalyzerOutputTableReader(
      project_id=self.project_id,
      analyzer=self.analyzer,
      output_id=output_id,
      store=self.store
    )


class PrimaryAnalyzerOutputTableReader(TableReader, BaseModel):
  project_id: str
  analyzer: AnalyzerInterface
  output_id: str
  store: Storage

  class Config:
    arbitrary_types_allowed = True

  @cached_property
  def parquet_path(self):
    return self.store.get_primary_output_parquet_path(self.project_id, self.analyzer.id, self.output_id)


class SecondaryAnalyzerOutputReaderGroupContext(AssetsReader, BaseModel):
  project_id: str
  primary_analyzer: AnalyzerInterface
  secondary_analyzer: SecondaryAnalyzerInterface
  store: Storage

  class Config:
    arbitrary_types_allowed = True

  def table(self, output_id: str) -> TableReader:
    return SecondaryAnalyzerOutputTableReader(
      project_id=self.project_id,
      primary_analyzer=self.primary_analyzer,
      secondary_analyzer=self.secondary_analyzer,
      output_id=output_id,
      store=self.store
    )


class SecondaryAnalyzerOutputTableReader(TableReader, BaseModel):
  project_id: str
  primary_analyzer: AnalyzerInterface
  secondary_analyzer: SecondaryAnalyzerInterface
  output_id: str
  store: Storage

  class Config:
    arbitrary_types_allowed = True

  @cached_property
  def parquet_path(self):
    return self.store.get_secondary_output_parquet_path(
      self.project_id, self.primary_analyzer.id,
      self.secondary_analyzer.id, self.output_id
    )


class SecondaryAnalyzerOutputWriter(TableWriter, BaseModel):
  project_id: str
  primary_analyzer: AnalyzerInterface
  secondary_analyzer: SecondaryAnalyzerInterface
  output_id: str
  store: Storage

  class Config:
    arbitrary_types_allowed = True

  @cached_property
  def parquet_path(self):
    return self.store.get_secondary_output_parquet_path(
      self.project_id, self.primary_analyzer.id,
      self.secondary_analyzer.id, self.output_id
    )
