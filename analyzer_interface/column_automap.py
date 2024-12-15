from pydantic import BaseModel

from .data_type_compatibility import get_data_type_compatibility_score
from .interface import DataType, InputColumn


class UserInputColumn(BaseModel):
    name: str
    data_type: DataType


def column_automap(
    user_columns: list[UserInputColumn], input_schema_columns: list[InputColumn]
):
    """
    Matches user-provided columns to the expected columns based on the name hints.

    The resulting dictionary is keyed by the expected input column name.
    """
    matches: dict[str, str] = {}
    for input_column in input_schema_columns:
        max_score = None
        best_match_user_column = None
        for user_column in user_columns:
            current_score = get_data_type_compatibility_score(
                input_column.data_type, user_column.data_type
            )

            # Don't consider type-incompatible columns
            if current_score is None:
                continue

            # Boost the score if we have a name hint match such that
            # - among similarly compatible matches, those with name hints are preferred
            # - among name hint matches, those with the best data type compatibility are preferred
            if any(
                check_name_hint(user_column.name, hint)
                for hint in input_column.name_hints
            ):
                current_score += 10

            if max_score is None or current_score > max_score:
                max_score = current_score
                best_match_user_column = user_column

        if best_match_user_column is not None:
            matches[input_column.name] = best_match_user_column.name

    return matches


def check_name_hint(name: str, hint: str):
    """
    Returns true if every word in the hint (split by spaces) is present in the name,
    in a case insensitive manner.
    """
    return all(word.lower().strip() in name.lower() for word in hint.split(" "))
