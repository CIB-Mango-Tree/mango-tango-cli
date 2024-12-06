import re

import polars as pl

from analyzer_interface.context import PrimaryAnalyzerContext

from .interface import (COL_AUTHOR_ID, COL_MESSAGE_ID, COL_MESSAGE_NGRAM_COUNT,
                        COL_MESSAGE_SURROGATE_ID, COL_MESSAGE_TEXT,
                        COL_MESSAGE_TIMESTAMP, COL_NGRAM_ID, COL_NGRAM_LENGTH,
                        COL_NGRAM_WORDS, OUTPUT_MESSAGE,
                        OUTPUT_MESSAGE_NGRAMS, OUTPUT_NGRAM_DEFS)


def main(context: PrimaryAnalyzerContext):
  input_reader = context.input()
  df_input = input_reader.preprocess(
    pl.read_parquet(input_reader.parquet_path)
  )
  df_input = df_input.filter(
    pl.col(COL_MESSAGE_TEXT).is_not_null() &
    (pl.col(COL_MESSAGE_TEXT) != "") &
    pl.col(COL_AUTHOR_ID).is_not_null() &
    (pl.col(COL_AUTHOR_ID) != "")
  )
  df_input = df_input.with_columns(
    (pl.int_range(pl.len()) + 1).alias(COL_MESSAGE_SURROGATE_ID)
  )

  def get_ngram_rows(ngrams_by_id: dict[str, int]):
    num_rows = df_input.height
    current_row = 0
    for row in df_input.iter_rows(named=True):
      tokens = tokenize(row[COL_MESSAGE_TEXT])
      for ngram in ngrams(tokens, 3, 5):
        serialized_ngram = serialize_ngram(ngram)
        if serialized_ngram not in ngrams_by_id:
          ngrams_by_id[serialized_ngram] = len(ngrams_by_id)
        ngram_id = ngrams_by_id[serialized_ngram]
        yield {
          COL_MESSAGE_SURROGATE_ID: row[COL_MESSAGE_SURROGATE_ID],
          COL_NGRAM_ID: ngram_id
        }
      current_row = current_row + 1
      if current_row % 100 == 0:
        print(
          current_row, "/", num_rows, "rows processed; found ",
          len(ngrams_by_id), "ngrams", end="\r"
        )

  ngrams_by_id: dict[str, int] = {}

  (
    pl.DataFrame(get_ngram_rows(ngrams_by_id))
      .group_by(COL_MESSAGE_SURROGATE_ID, COL_NGRAM_ID)
      .agg(pl.count().alias(COL_MESSAGE_NGRAM_COUNT))
      .write_parquet(context.output(OUTPUT_MESSAGE_NGRAMS).parquet_path)
  )

  (
    pl.DataFrame({
      COL_NGRAM_ID: list(ngrams_by_id.values()),
      COL_NGRAM_WORDS: list(ngrams_by_id.keys())
    })
      .with_columns([
        pl.col(COL_NGRAM_WORDS)
          .str.split(" ")
          .list.len()
          .alias(COL_NGRAM_LENGTH)
      ])
      .write_parquet(context.output(OUTPUT_NGRAM_DEFS).parquet_path)
  )

  (
    df_input.select(
      [COL_MESSAGE_SURROGATE_ID, COL_MESSAGE_ID, COL_MESSAGE_TEXT, COL_AUTHOR_ID, COL_MESSAGE_TIMESTAMP])
      .write_parquet(context.output(OUTPUT_MESSAGE).parquet_path)
  )


def tokenize(input: str) -> list[str]:
  """Generate words from input string."""
  return re.split(r'\W+', input.lower())


def ngrams(tokens: list[str], min: int, max: int):
  """Generate n-grams from list of tokens."""
  for i in range(len(tokens) - min + 1):
    for n in range(min, max + 1):
      if i + n > len(tokens):
        break
      yield tokens[i:i + n]


def serialize_ngram(ngram: list[str]) -> str:
  """Generates a string that uniquely represents an ngram"""
  return " ".join(ngram)
