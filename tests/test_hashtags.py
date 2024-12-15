
from analyzers.hashtags.main import gini, main
import polars as pl

# test dataset with the following properties:
# five unique users
# five posts (i.e. five rows) over two hours
# 6 hashtags in the first hour (uniform, gini == 0)
# 10 hashtags in the second hour (skewed, gini > 0)
test_df = pl.DataFrame({
    "user_id": [1, 2, 3, 4, 5],  # we have five users
    "hashtags": [
        "['a', 'b', 'c']",
        "[]",
        "['a', 'b', 'c']",
        "['d', 'e', 'f', 'g', 'h']",
        "['d', 'd', 'd', 'd', 'd]",  # --> increase in #d
    ],
    "time": [  # spanning 2 hours
        "2024-01-01 01:00:00",
        "2024-01-01 01:05:00",
        "2024-01-01 01:15:00",
        "2024-01-01 02:15:00",
        "2024-01-01 02:00:00",
    ]
})

# convert the time column to datetime
test_df = test_df.with_columns(
    pl.col("time").str.to_datetime()
)


class DummyOutput():
    "Dummy output object that contains the parquet path attribute."
    def __init__(self):
        self.parquet_path = "test_hashtags.parquet"


class AnalyzerContextDummy():

    """Dummy object that allows us to access test data and output a dummy output object."""

    def get_test_df(self):
        return test_df
    
    def output(self, output_id: str):
        return DummyOutput()


def test_gini():

    # one element occurs very often
    high_gini_ds = {
        "a": 1, # element and the count of the element occurences
        "b": 1,
        "c": 1,
        "d": 1,
        "e": 1,
        "f": 1,
        "g": 1,
        "h": 1,
        "i": 1,
        "j": 10
    }

    # one element occurs moderately often
    mid_gini_ds = {
        "a": 1, # element and the count of the element occurences
        "b": 1,
        "c": 1,
        "d": 1,
        "e": 1,
        "f": 1,
        "g": 1,
        "h": 1,
        "i": 1,
        "j": 4
    }

    # all elements occur equally often
    low_gini_ds = {
        "a": 1,
        "b": 1,
        "c": 1,
        "d": 1,
        "e": 1,
        "f": 1,
        "g": 1,
        "h": 1,
        "i": 1,
        "j": 1
    }

    # create a a list of elements from the above dictionaries
    high_gini_data = [e for e, c in high_gini_ds.items() for _ in range(c)]
    mid_gini_data = [e for e, c in mid_gini_ds.items() for _ in range(c)]
    low_gini_data = [e for e, c in low_gini_ds.items() for _ in range(c)]

    # test that the gini coefficient is higher for more skewed data
    assert gini(high_gini_data) > gini(low_gini_data), "Gini coefficient should be higher for skewed data than uniform data"
    assert gini(mid_gini_data) > gini(low_gini_data), "Gini coefficient should be higher for skewed data than uniform data"
    assert gini(high_gini_data) > gini(mid_gini_data), "Gini coefficient should be higher for more skewed data"
    
    # test that the gini coefficient is 0 for uniform data
    assert gini(low_gini_data) == 0, "Gini coefficient should be 0 for uniform data"


def test_main():

    context = AnalyzerContextDummy()

    main(context)

    return None