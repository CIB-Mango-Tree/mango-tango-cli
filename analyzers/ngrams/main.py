import polars as pl
import re


AUTHOR__ID = "user_id"
MESSAGE__ID = "message_id"
MESSAGE__TEXT = "message_text"
MESSAGE__NGRAM_COUNT = "count"
NGRAM__ID = "ngram_id"
NGRAM__WORDS = "words"
NGRAM__LENGTH = "n"


def analyze_ngrams(df_input: pl.DataFrame):
  df_input = df_input.filter(pl.col(MESSAGE__TEXT).is_not_null())

  def get_ngram_rows(ngrams_by_id: dict[str, int]):
    num_rows = df_input.height
    current_row = 0
    for row in df_input.iter_rows(named=True):
      tokens = tokenize(row[MESSAGE__TEXT])
      for ngram in ngrams(tokens, 3, 5):
        serialized_ngram = serialize_ngram(ngram)
        if serialized_ngram not in ngrams_by_id:
          ngrams_by_id[serialized_ngram] = len(ngrams_by_id)
        ngram_id = ngrams_by_id[serialized_ngram]
        yield {
          MESSAGE__ID: row[MESSAGE__ID],
          NGRAM__ID: ngram_id
        }
      current_row = current_row + 1
      if current_row % 100 == 0:
        print(
          current_row, "/", num_rows, "rows processed; found ",
          len(ngrams_by_id), "ngrams", end="\r"
        )

  ngrams_by_id: dict[str, int] = {}

  df_message_ngrams = (
    pl.DataFrame(get_ngram_rows(ngrams_by_id))
      .group_by(MESSAGE__ID, NGRAM__ID)
      .agg(pl.count().alias(MESSAGE__NGRAM_COUNT))
  )
  df_ngrams = pl.DataFrame({
    NGRAM__ID: list(ngrams_by_id.values()),
    NGRAM__WORDS: list(ngrams_by_id.keys())
  }).with_columns([
    pl.col(NGRAM__WORDS)
      .str.split(" ")
      .list.len()
      .alias(NGRAM__LENGTH)
  ])
  df_message_authors = df_input.select(
    [AUTHOR__ID, MESSAGE__ID])

  return {
    "message_ngrams": df_message_ngrams,
    "ngrams": df_ngrams,
    "message_authors": df_message_authors
  }


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
