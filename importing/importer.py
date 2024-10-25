from abc import ABC, abstractmethod
from typing import Optional, TypeVar

import polars as pl


class IImporterPreload(ABC):
  @abstractmethod
  def get_preview_dataframe(self) -> pl.DataFrame:
    pass


PreviewType = TypeVar("PreviewType", bound=IImporterPreload)


class Importer[PreviewType](ABC):
  @property
  @abstractmethod
  def name(self) -> str:
    pass

  @abstractmethod
  def sniff(self, input_path: str) -> bool:
    pass

  @abstractmethod
  def preload(self, input_path: str, n_records: int) -> Optional[PreviewType]:
    pass

  @abstractmethod
  def import_data(self, input_path: str, output_path: str, preview: PreviewType) -> None:
    pass
