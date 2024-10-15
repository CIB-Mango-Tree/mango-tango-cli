import polars as pl

from analyzer_interface import UserInputColumn
from preprocessing.series_semantic import infer_series_semantic
from storage import Project


def input_preview(df: pl.DataFrame):
  user_columns = get_user_columns(df)
  print(df)
  print("Inferred column semantics:")
  for col in user_columns:
    print(f"  {col.name}: {col.data_type}")


def get_user_columns(df: pl.DataFrame):
  return [
    UserInputColumn(name=col, data_type=semantic.data_type)
    for col in df.columns
    if (semantic := infer_series_semantic(df[col])) is not None
  ]


class ProjectInstance(Project):
  input: pl.DataFrame

  class Config:
    arbitrary_types_allowed = True
