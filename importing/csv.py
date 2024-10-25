from csv import Sniffer, Dialect
from typing import Any

import polars as pl
from pydantic import BaseModel

from .importer import IImporterPreload, Importer


class CSVImporter(Importer["CSVPreload"]):
  @property
  def name(self) -> str:
    return "CSV"

  def sniff(self, input_path: str) -> bool:
    return input_path.endswith(".csv")

  def preload(self, input_path: str, n_records: int):
    with open(input_path, "r", encoding="utf8") as file:
      dialect = Sniffer().sniff(file.read(65536))
    df = pl.read_csv(
      input_path, **self._get_csv_args(dialect), n_rows=n_records
    )
    print(dialect)
    return CSVPreload(preview_df=df, dialect=dialect)

  def import_data(self, input_path: str, output_path: str, preload: "CSVPreload") -> pl.DataFrame:
    dialect: Dialect = preload.dialect
    lazyframe = pl.scan_csv(
      input_path, **self._get_csv_args(dialect)
    )
    lazyframe.sink_parquet(output_path)

  @staticmethod
  def _get_csv_args(dialect: Dialect) -> dict[str, str]:
    return {
      "separator": dialect.delimiter,
      "quote_char": dialect.quotechar,
      "ignore_errors": True,
      "has_header": True,
      "truncate_ragged_lines": True,
    }


class CSVPreload(IImporterPreload, BaseModel):
  preview_df: pl.DataFrame
  dialect: Any

  class Config:
    arbitrary_types_allowed = True

  def get_preview_dataframe(self) -> pl.DataFrame:
    return self.preview_df
