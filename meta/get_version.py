import os
from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=1)
def get_version():
    root_path = str(Path(__file__).resolve().parent.parent)
    version_path = os.path.join(root_path, "VERSION")
    try:
        with open(version_path, "r") as version_file:
            return version_file.read().strip()
    except FileNotFoundError:
        return None
    except PermissionError:  # Swallow this for now
        return None


def is_distributed():
    return get_version() is not None


def is_development():
    return not is_distributed()
