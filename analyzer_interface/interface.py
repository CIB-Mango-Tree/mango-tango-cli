from typing import Callable, Literal, Optional

from pydantic import BaseModel


class AnalyzerInterface(BaseModel):
  id: str
  """
  The static ID for the analyzer that, with the version, uniquely identifies the
  analyzer and will be stored as metadata as part of the output data.
  """

  version: str
  """
  The version ID for the analyzer. In future, we may choose to support output
  migration between versions of the same analyzer.
  """

  name: str
  """
  The short human-readable name of the analyzer.
  """

  short_description: str
  """
  A short, one-liner description of what the analyzer does.
  """

  long_description: Optional[str] = None
  """
  A longer description of what the analyzer does that will be shown separately.
  """

  input: "AnalyzerInput"
  """
  Specifies the input data schema for the analyzer.
  """

  outputs: list["AnalyzerOutput"]
  """
  Specifies the output data schema for the analyzer.
  """

  entry_point: Callable
  """
  The entry point should be a function that accepts the input dataframe and
  returns a dictionary of output dataframes
  """


class AnalyzerInput(BaseModel):
  columns: list["InputColumn"]


class AnalyzerOutput(BaseModel):
  id: str
  """
  Uniquely identifies the output data schema for the analyzer. The analyzer
  must include this key in the output dictionary.
  """

  name: str
  """The human-friendly for the output."""

  description: Optional[str] = None

  columns: list["OutputColumn"]


DataType = Literal[
  "text", "integer", "float", "boolean", "datetime", "identifier", "url"
]
"""
The semantic data type for a data column. This is not quite the same as
structural data types like polars or pandas or even arrow types, but they
represent how the data is intended to be interpreted.

- `text` is expected to be a free-form human-readable text content.
- `integer` and `float` are meant to be manipulated arithmetically.
- `boolean` is a binary value.
- `datetime` represents time and are meant to be manipulated as time values.
- `identifier` is a unique identifier for a record. It is not expected to be manipulated in any way.
- `url` is a string that represents a URL.
"""


class Column(BaseModel):
  name: str
  description: Optional[str] = None
  data_type: DataType


class InputColumn(Column):
  name_hints: list[str] = []
  """
  Specifies a list of space-separated words that are likely to be found in the
  column name of the user-provided data. This is used to help the user map the
  input columns to the expected columns.

  Any individual hint matching is sufficient for a match to be called. The hint
  in turn is matched if every word matches some part of the column name.
  """


class OutputColumn(Column):
  pass
