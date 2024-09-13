import polars as pl
from pydantic import BaseModel
from typing import Union, Type, Callable


class SeriesSemantic(BaseModel):
  semantic_name: str
  column_type: Union[Type[pl.DataType], Callable[[pl.DataType], bool]]
  prevalidate: Callable[[pl.Series], bool] = lambda s: True
  try_convert: Callable[[pl.Series], pl.Series]
  validate_result: Callable[[pl.Series], pl.Series] = lambda s: s.is_not_null()

  def check(self, series: pl.Series, threshold: float = 0.8, sample_size: int = 100):
    if not self.check_type(series):
      return False

    sample = sample_series(series, sample_size)
    try:
      if not self.prevalidate(sample):
        return False
      result = self.try_convert(sample)
    except Exception:
      return False
    return self.validate_result(result).sum() / sample.len() > threshold

  def check_type(self, series: pl.Series):
    if isinstance(self.column_type, type):
      return isinstance(series.dtype, self.column_type)
    return self.column_type(series.dtype)


datetime_string = SeriesSemantic(
  semantic_name="datetime",
  column_type=pl.String,
  try_convert=lambda s: s.str.strptime(pl.Datetime, strict=False)
)

timestamp_seconds = SeriesSemantic(
  semantic_name="timestamp_seconds",
  column_type=lambda dt: dt.is_numeric(),
  prevalidate=lambda s: s.gt(946_684_800) & s.lt(2_524_608_000),
  try_convert=lambda s: (s * 1_000).cast(pl.Datetime(time_unit="ms")),
)

timestamp_milliseconds = SeriesSemantic(
  semantic_name="timestamp_milliseconds",
  column_type=lambda dt: dt.is_numeric(),
  prevalidate=lambda s: s.gt(946_684_800_000) & s.lt(2_524_608_000_000),
  try_convert=lambda s: s.cast(pl.Datetime(time_unit="ms")),
)

url = SeriesSemantic(
  semantic_name="url",
  column_type=pl.String,
  try_convert=lambda s: s.str.strip_chars(),
  validate_result=lambda s: s.str.count_matches("^https?://").gt(0)
)

identifier = SeriesSemantic(
  semantic_name="identifier",
  column_type=pl.String,
  try_convert=lambda s: s.str.strip_chars(),
  validate_result=lambda s: s.str.count_matches(r"^@?[A-Za-z0-9_.:-]+$").eq(1)
)


def infer_series_semantic(series: pl.Series, *, threshold: float = 0.8, sample_size=100):
  for semantic in [datetime_string, timestamp_milliseconds, timestamp_seconds, url, identifier]:
    if semantic.check(series, threshold=threshold, sample_size=sample_size):
      return semantic
  return None


def sample_series(series: pl.Series, n: int = 100):
  if series.len() < n:
    return series
  return series.sample(n, seed=0)
