from typing import Optional

import polars as pl

from analyzer_interface import UserInputColumn as BaseUserInputColumn
from preprocessing.series_semantic import SeriesSemantic, infer_series_semantic
from storage import TableStats
from terminal_tools import print_ascii_table


class UserInputColumn(BaseUserInputColumn):
    semantic: SeriesSemantic
    data: pl.Series

    def head(self, n: int = 10):
        return UserInputColumn(
            name=self.name,
            data_type=self.data_type,
            semantic=self.semantic,
            data=self.data.head(n),
        )

    def apply_semantic_transform(self):
        return self.semantic.try_convert(self.data)

    class Config:
        arbitrary_types_allowed = True


def input_preview(df: pl.DataFrame, stats: Optional[TableStats] = None):
    user_columns = get_user_columns(df)
    print_ascii_table(
        [[preview_value(cell) for cell in row] for row in df.head(10).iter_rows()],
        header=df.columns,
    )
    if stats is not None and stats.num_rows > df.height:
        print(f"(Total {stats.num_rows} rows)")

    print("Inferred column semantics:")
    print_ascii_table(
        rows=[[col.name, col.semantic.semantic_name] for col in user_columns],
        header=["Column", "Semantic"],
    )


def preview_value(value):
    if isinstance(value, str):
        if len(value) > 20:
            return value[:20] + "..."
        return value
    if value is None:
        return "(N/A)"
    return value


def get_user_columns(df: pl.DataFrame):
    return [
        UserInputColumn(
            name=col, data_type=semantic.data_type, semantic=semantic, data=df[col]
        )
        for col in df.columns
        if (semantic := infer_series_semantic(df[col])) is not None
    ]
