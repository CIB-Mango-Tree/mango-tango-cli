import polars as pl

from analyzer_interface import UserInputColumn as BaseUserInputColumn
from preprocessing.series_semantic import infer_series_semantic, SeriesSemantic
from storage import Project


class UserInputColumn(BaseUserInputColumn):
  semantic: SeriesSemantic
  data: pl.Series

  def head(self, n: int = 10):
    return UserInputColumn(
      name=self.name,
      data_type=self.data_type,
      semantic=self.semantic,
      data=self.data.head(n)
    )

  def apply_semantic_transform(self):
    return self.semantic.try_convert(self.data)

  class Config:
    arbitrary_types_allowed = True


def input_preview(df: pl.DataFrame):
  user_columns = get_user_columns(df)
  print(df)
  print("Inferred column semantics:")
  for col in user_columns:
    print(f"  {col.name}: {col.data_type}")


def get_user_columns(df: pl.DataFrame):
  return [
    UserInputColumn(name=col, data_type=semantic.data_type,
                    semantic=semantic, data=df[col])
    for col in df.columns
    if (semantic := infer_series_semantic(df[col])) is not None
  ]


class ProjectInstance(Project):
  input: pl.DataFrame

  class Config:
    arbitrary_types_allowed = True
