from analyzer_interface import (AnalyzerInput, AnalyzerInterface,
                                AnalyzerOutput, InputColumn, OutputColumn)

COL_AUTHOR_ID = "user_id"
COL_MESSAGE_ID = "message_id"
COL_MESSAGE_TEXT = "message_text"
COL_MESSAGE_NGRAM_COUNT = "count"
COL_NGRAM_ID = "ngram_id"
COL_NGRAM_WORDS = "words"
COL_NGRAM_LENGTH = "n"

OUTPUT_MESSAGE_NGRAMS = "message_ngrams"
OUTPUT_NGRAM_DEFS = "ngrams"
OUTPUT_MESSAGE_AUTHORS = "message_authors"

interface = AnalyzerInterface(
  id="ngrams",
  version="0.1.0",
  name="N-gram Analysis",
  short_description="Extracts n-grams from text data",
  long_description="""
The n-gram analysis extract n-grams (sequences of n words) from the text data
in the input and counts the occurrences of each n-gram in each message, linking
the message author to the ngram frequency.

The result can be used to see if certain word sequences are more common in
the corpus of text, and whether certain authors use these sequences more often.
  """,
  input=AnalyzerInput(columns=[
    InputColumn(
      name=COL_AUTHOR_ID,
      data_type="identifier",
      description="The unique identifier of the author of the message",
      name_hints=["author", "user", "poster", "username",
                  "screen name", "user name", "name", "email"]
    ),
    InputColumn(
      name=COL_MESSAGE_ID,
      data_type="identifier",
      description="The unique identifier of the message",
      name_hints=["post", "message", "comment",
                  "text", "retweet id", "tweet"]
    ),
    InputColumn(
      name=COL_MESSAGE_TEXT,
      data_type="text",
      description="The text content of the message",
      name_hints=["message", "text", "comment",
                  "post", "body", "content", "tweet"]
    )
  ]),
  outputs=[
    AnalyzerOutput(
      id=OUTPUT_MESSAGE_NGRAMS,
      name="N-gram count per message",
      columns=[
        OutputColumn(name=COL_MESSAGE_ID, data_type="identifier"),
        OutputColumn(name=COL_NGRAM_ID, data_type="identifier"),
        OutputColumn(name=COL_MESSAGE_NGRAM_COUNT, data_type="integer")
      ]
    ),
    AnalyzerOutput(
      id=OUTPUT_NGRAM_DEFS,
      name="N-gram definitions",
      description="The word compositions of each unique n-gram",
      columns=[
        OutputColumn(name=COL_NGRAM_ID, data_type="identifier"),
        OutputColumn(name=COL_NGRAM_WORDS, data_type="text"),
        OutputColumn(name=COL_NGRAM_LENGTH, data_type="integer")
      ]
    ),
    AnalyzerOutput(
      id=OUTPUT_MESSAGE_AUTHORS,
      name="Message authorship",
      description="Message authorship",
      columns=[
        OutputColumn(name=COL_AUTHOR_ID, data_type="identifier"),
        OutputColumn(name=COL_MESSAGE_ID, data_type="identifier")
      ]
    )
  ]
)
