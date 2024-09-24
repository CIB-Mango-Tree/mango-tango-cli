from analyzer_interface import (AnalyzerInput, AnalyzerInterface,
                                AnalyzerOutput, InputColumn, OutputColumn)

from .main import (MESSAGE__ID, MESSAGE__TEXT,
                   NGRAM__ID, NGRAM__LENGTH,
                   NGRAM__WORDS, MESSAGE__NGRAM_COUNT, AUTHOR__ID, analyze_ngrams)

interface = AnalyzerInterface(
  id="ngrams",
  version="0.1.0",
  name="ngrams",
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
      name=AUTHOR__ID,
      data_type="identifier",
      description="The unique identifier of the author of the message",
      name_hints=["author", "user", "poster", "username",
                  "screen name", "user name", "name", "email"]
    ),
    InputColumn(
      name=MESSAGE__ID,
      data_type="identifier",
      description="The unique identifier of the message",
      name_hints=["post", "message", "comment",
                  "text", "retweet id", "tweet"]
    ),
    InputColumn(
      name=MESSAGE__TEXT,
      data_type="text",
      description="The text content of the message",
      name_hints=["message", "text", "comment",
                  "post", "body", "content", "tweet"]
    )
  ]),
  outputs=[
    AnalyzerOutput(
      id="message_ngrams",
      name="N-gram count per message",
      columns=[
        OutputColumn(name=MESSAGE__ID, data_type="identifier"),
        OutputColumn(name=NGRAM__ID, data_type="identifier"),
        OutputColumn(name=MESSAGE__NGRAM_COUNT, data_type="integer")
      ]
    ),
    AnalyzerOutput(
      id="ngrams",
      name="N-gram definitions",
      description="The word compositions of each unique n-gram",
      columns=[
        OutputColumn(name=NGRAM__ID, data_type="identifier"),
        OutputColumn(name=NGRAM__WORDS, data_type="text"),
        OutputColumn(name=NGRAM__LENGTH, data_type="integer")
      ]
    ),
    AnalyzerOutput(
      id="message_authors",
      name="Message authorship",
      description="Message authorship",
      columns=[
        OutputColumn(name=AUTHOR__ID, data_type="identifier"),
        OutputColumn(name=MESSAGE__ID, data_type="identifier")
      ]
    )
  ],
  entry_point=analyze_ngrams
)
