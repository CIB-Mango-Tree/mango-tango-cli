from .interface import DataType

data_type_mapping_preference: dict[DataType, list[list[DataType]]] = {
    "text": [["text"], ["identifier", "url"]],
    "integer": [["integer"]],
    "float": [["float", "integer"]],
    "boolean": [["boolean"]],
    "datetime": [["datetime"]],
    "time": [["time"], ["datetime"]],
    "identifier": [["identifier"], ["url", "datetime"], ["integer"], ["text"]],
    "url": [["url"]],
}
"""
For each data type, a list of lists of data types that are considered compatible
with it. The first list is the most preferred, the last list is the least. The
items in each list are considered equally compatible.
"""


def get_data_type_compatibility_score(
    expected_data_type: DataType, actual_data_type: DataType
):
    """
    Returns a score for the compatibility of the actual data type with the
    expected data type. Higher (less negative) scores are better.
    `None` means the data types are not compatible.
    """
    if expected_data_type == actual_data_type:
        return 0

    for i, preference_list in enumerate(
        data_type_mapping_preference[expected_data_type]
    ):
        if actual_data_type in preference_list:
            return -(i + 1)

    return None
