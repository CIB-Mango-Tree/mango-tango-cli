import pandas as pd
from pandas.core.dtypes.base import ExtensionDtype
from pydantic import BaseModel
from typing import Union, Type, Callable


class SeriesSemantic(BaseModel):
  semantic_name: str
  column_type: Union[Type[ExtensionDtype],
                     Callable[[pd.Series], bool]]
  prevalidate: Callable[[pd.Series], bool] = lambda s: True
  try_convert: Callable[[pd.Series], pd.Series]
  validate_result: Callable[[pd.Series], pd.Series] = lambda s: s.notnull()

  def check(self, series: pd.Series, threshold: float = 0.8, sample_size: int = 100):
    if not self.check_type(series):
      return False

    sample = sample_series_non_null(series, sample_size)
    try:
      if not self.prevalidate(sample):
        return False
      result = self.try_convert(sample)
    except Exception:
      return False
    return self.validate_result(result).sum() / sample.count() > threshold

  def check_type(self, series: pd.Series):
    if isinstance(self.column_type, type):
      return isinstance(series.dtype, self.column_type)
    return self.column_type(series.dtype)


datetime_string = SeriesSemantic(
  semantic_name="datetime",
  column_type=lambda dt: pd.api.types.is_string_dtype(dt),
  try_convert=lambda s: pd.to_datetime(s, errors='coerce')
)

timestamp_seconds = SeriesSemantic(
  semantic_name="timestamp_seconds",
  column_type=lambda dt: pd.api.types.is_numeric_dtype(dt),
  prevalidate=lambda s: (s > 946_684_800) & (s < 2_524_608_000),
  try_convert=lambda s: pd.to_datetime(s * 1_000, unit='ms', errors='coerce'),
)

timestamp_milliseconds = SeriesSemantic(
  semantic_name="timestamp_milliseconds",
  column_type=lambda dt: pd.api.types.is_numeric_dtype(dt),
  prevalidate=lambda s: (s > 946_684_800_000) & (s < 2_524_608_000_000),
  try_convert=lambda s: pd.to_datetime(s, unit='ms', errors='coerce'),
)

url = SeriesSemantic(
  semantic_name="url",
  column_type=lambda dt: pd.api.types.is_string_dtype(dt),
  try_convert=lambda s: s.str.strip(),
  validate_result=lambda s: s.str.contains("^https?://", regex=True)
)

identifier = SeriesSemantic(
  semantic_name="identifier",
  column_type=lambda dt: pd.api.types.is_string_dtype(dt),
  try_convert=lambda s: s.str.strip(),
  validate_result=lambda s: s.str.contains(r"^@?[A-Za-z0-9_.:-]+$", regex=True)
)


def infer_series_semantic(series: pd.Series, *, threshold: float = 0.8, sample_size=100):
  for semantic in [datetime_string, timestamp_milliseconds, timestamp_seconds, url, identifier]:
    if semantic.check(series, threshold=threshold, sample_size=sample_size):
      return semantic
  return None


def sample_series_non_null(series: pd.Series, n: int = 100):
  series = series.dropna()
  if len(series) < n:
    return series

  return series.sample(n, random_state=0)
