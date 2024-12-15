from abc import ABC, abstractmethod
from typing import TypeVar

import polars as pl
from dash import Dash
from pydantic import BaseModel

from .interface import SecondaryAnalyzerInterface


class PrimaryAnalyzerContext(ABC, BaseModel):
    temp_dir: str
    """
  Gets the temporary directory that the module can freely write content to
  during its lifetime. This directory will not persist between runs.
  """

    @abstractmethod
    def input(self) -> "InputTableReader":
        """
        Gets the input reader context.

        **Note that this is in function form** even though one input is expected,
        in anticipation that we may want to support multiple inputs in the future.
        """
        pass

    @abstractmethod
    def output(self, output_id: str) -> "TableWriter":
        """
        Gets the output writer context for the specified output ID.
        """
        pass


class BaseDerivedModuleContext(ABC, BaseModel):
    """
    Common interface for secondary analyzers and web presenters runtime contexts.
    """

    temp_dir: str
    """
  Gets the temporary directory that the module can freely write content to
  during its lifetime. This directory will not persist between runs.
  """

    @property
    @abstractmethod
    def base(self) -> "AssetsReader":
        """
        Gets the base primary analyzer's context, which lets you inspect and load its
        outputs.
        """
        pass

    @abstractmethod
    def dependency(
        self, secondary_interface: SecondaryAnalyzerInterface
    ) -> "AssetsReader":
        """
        Gets the context of a secondary analyzer the current module depends on, which
        lets you inspect and load its outputs.
        """
        pass


class WebPresenterContext(BaseDerivedModuleContext):
    dash_app: Dash
    """
  The Dash app that is being built.
  """

    @property
    @abstractmethod
    def state_dir(self) -> str:
        """
        Gets the directory where the web presenter can store state that persists
        between runs. This state space is unique for each
        project/primary analyzer/web presenter combination.
        """
        pass

    class Config:
        arbitrary_types_allowed = True


class SecondaryAnalyzerContext(BaseDerivedModuleContext):
    @abstractmethod
    def output(self, output_id: str) -> "TableWriter":
        """
        Gets the output writer context
        """
        pass


class AssetsReader(ABC):
    @abstractmethod
    def table(self, output_id: str) -> "TableReader":
        """
        Gets the table reader for the specified output.
        """
        pass


class TableReader(ABC):
    @property
    @abstractmethod
    def parquet_path(self) -> str:
        """
        Gets the path to the table's parquet file. The module should expect a parquet
        file here.
        """
        pass


PolarsDataFrameLike = TypeVar("PolarsDataFrameLike", bound=pl.DataFrame)


class InputTableReader(TableReader):
    @abstractmethod
    def preprocess[
        PolarsDataFrameLike
    ](self, df: PolarsDataFrameLike) -> PolarsDataFrameLike:
        """
        Given the manually loaded user input dataframe, apply column mapping and
        semantic transformations to give the input dataframe that the analyzer
        expects.
        """
        pass


class TableWriter(ABC):
    @property
    @abstractmethod
    def parquet_path(self) -> str:
        """
        Gets the path to the table's parquet file. The module should write a parquet
        file to it.
        """
        pass
