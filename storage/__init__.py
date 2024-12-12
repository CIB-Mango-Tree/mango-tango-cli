import math
import os
import re
import shutil
from typing import Callable, Iterable, Literal, Optional

import platformdirs
import polars as pl
import pyarrow.parquet as pq
from filelock import FileLock
from pydantic import BaseModel
from tinydb import Query, TinyDB

from analyzer_interface.interface import AnalyzerOutput

STORAGE_VERSION = 1


class Project(BaseModel):
  class_: Literal["project"] = "project"
  id: str
  display_name: str


class Settings(BaseModel):
  class_: Literal["settings"] = "settings"
  export_chunk_size: Optional[int | Literal[False]] = None


SupportedOutputExtension = Literal["parquet", "csv", "xlsx", "json"]


class Storage:
  def __init__(self, *, app_name: str, app_author: str):
    self.user_data_dir = platformdirs.user_data_dir(
      appname=app_name, appauthor=app_author, ensure_exists=True
    )
    self.temp_dir = platformdirs.user_cache_dir(
      appname=app_name, appauthor=app_author, ensure_exists=True
    )
    self.db = TinyDB(self._get_db_path())
    with self._lock_database():
      self._ensure_database_version()

  def init_project(self, *, display_name: str, input_temp_file: str):
    with self._lock_database():
      project_id = self._find_unique_project_id(display_name)
      project = Project(id=project_id, display_name=display_name)
      self.db.insert(project.model_dump())

    project_dir = self._get_project_path(project_id)
    os.makedirs(project_dir, exist_ok=True)

    os.rename(input_temp_file, self._get_project_input_path(project_id))
    return project

  def list_projects(self):
    q = Query()
    projects = self.db.search(q["class_"] == "project")
    return sorted(
      (Project(**project) for project in projects),
      key=lambda project: project.display_name
    )

  def delete_project(self, project_id: str):
    with self._lock_database():
      q = Query()
      self.db.remove((q["id"] == project_id) & (q["class_"] == "project"))
    project_path = self._get_project_path(project_id)
    shutil.rmtree(project_path, ignore_errors=True)

  def load_project_input(self, project_id: str, *, n_records: Optional[int] = None):
    input_path = self._get_project_input_path(project_id)
    return pl.read_parquet(input_path, n_rows=n_records)

  def get_project_input_stats(self, project_id: str):
    input_path = self._get_project_input_path(project_id)
    num_rows = pl.scan_parquet(input_path).select(pl.count()).collect().item()
    return TableStats(num_rows=num_rows)

  def save_project_primary_outputs(self, project_id: str, analyzer_id: str, outputs: dict[str, pl.DataFrame]):
    for output_id, output_df in outputs.items():
      self._save_output(
        os.path.join(self._get_project_primary_output_root_path(
          project_id, analyzer_id), output_id),
        output_df,
        "parquet",
      )

  def save_project_secondary_outputs(self, project_id: str, analyzer_id: str, secondary_id: str, outputs: dict[str, pl.DataFrame]):
    for output_id, output_df in outputs.items():
      self._save_output(
        os.path.join(self._get_project_secondary_output_root_path(
          project_id, analyzer_id, secondary_id), output_id),
        output_df,
        "parquet",
      )

  def save_project_secondary_output(self, project_id: str, analyzer_id: str, secondary_id: str, output_id: str, output_df: pl.DataFrame, extension: SupportedOutputExtension):
    root_path = self._get_project_secondary_output_root_path(
      project_id, analyzer_id, secondary_id)
    self._save_output(
      os.path.join(root_path, output_id), output_df, extension,
    )

  def _save_output(self, output_path_without_extension, output_df: pl.DataFrame | pl.LazyFrame, extension: SupportedOutputExtension,):
    output_df = output_df.lazy()
    os.makedirs(os.path.dirname(output_path_without_extension), exist_ok=True)
    output_path = f"{output_path_without_extension}.{extension}"
    if extension == "parquet":
      output_df.sink_parquet(output_path)
    elif extension == "csv":
      output_df.sink_csv(output_path)
    elif extension == "xlsx":
      output_df.collect().write_excel(output_path)
    elif extension == "json":
      output_df.collect().write_json(output_path)
    else:
      raise ValueError(f"Unsupported format: {extension}")
    return output_path

  def load_project_primary_output(self, project_id: str, analyzer_id: str, output_id: str):
    output_path = self.get_primary_output_parquet_path(
      project_id, analyzer_id, output_id)
    return pl.read_parquet(output_path)

  def get_primary_output_parquet_path(self, project_id: str, analyzer_id: str, output_id: str):
    return os.path.join(
      self._get_project_primary_output_root_path(project_id, analyzer_id),
      f"{output_id}.parquet"
    )

  def load_project_secondary_output(self, project_id: str, analyzer_id: str, secondary_id: str, output_id: str):
    output_path = self.get_secondary_output_parquet_path(
      project_id, analyzer_id, secondary_id, output_id)
    return pl.read_parquet(output_path)

  def get_secondary_output_parquet_path(self, project_id: str, analyzer_id: str, secondary_id: str, output_id: str):
    return os.path.join(self._get_project_secondary_output_root_path(
        project_id, analyzer_id, secondary_id), f"{output_id}.parquet")

  def export_project_primary_output(
    self, project_id: str, analyzer_id: str, output_id: str, *,
    extension: SupportedOutputExtension, spec: AnalyzerOutput,
    export_chunk_size: Optional[int] = None
  ):
    return self._export_output(
      self.get_primary_output_parquet_path(
        project_id, analyzer_id, output_id),
      os.path.join(
        self._get_project_exports_root_path(project_id, analyzer_id),
        output_id
      ),
      extension=extension,
      spec=spec,
      export_chunk_size=export_chunk_size
    )

  def export_project_secondary_output(
    self, project_id: str, analyzer_id: str, secondary_id: str, output_id: str, *,
    extension: SupportedOutputExtension, spec: AnalyzerOutput,
    export_chunk_size: Optional[int] = None
  ):
    exported_path = os.path.join(
      self._get_project_exports_root_path(project_id, analyzer_id),
      secondary_id
        if secondary_id == output_id
        else f"{secondary_id}__{output_id}"
    )
    return self._export_output(
      self.get_secondary_output_parquet_path(
        project_id, analyzer_id, secondary_id, output_id),
      exported_path,
      extension=extension,
      spec=spec,
      export_chunk_size=export_chunk_size
    )

  def _export_output(
    self, input_path: str, output_path: str, *,
    extension: SupportedOutputExtension, spec: AnalyzerOutput,
    export_chunk_size: Optional[int] = None
  ):
    if not export_chunk_size:
      df = pl.scan_parquet(input_path)
      self._save_output(output_path, spec.transform_output(df), extension)
      return f"{output_path}.{extension}"

    with pq.ParquetFile(input_path) as reader:
      num_chunks = math.ceil(reader.metadata.num_rows / export_chunk_size)
      get_batches = (
        df
        for batch in reader.iter_batches()
        if (df := pl.from_arrow(batch)) is not None
      )
      for chunk_id, chunk in enumerate(collect_dataframe_chunks(get_batches, export_chunk_size)):
        chunk = spec.transform_output(chunk)
        self._save_output(f"{output_path}_{chunk_id}", chunk, extension)
        yield chunk_id / num_chunks
      return f"{output_path}_[*].{extension}"

  def list_project_analyses(self, project_id: str):
    project_path = self._get_project_path(project_id)
    try:
      analyzers = os.listdir(os.path.join(project_path, "analyzers"))
      return analyzers
    except FileNotFoundError:
      return []

  def list_project_secondary_analyses(self, project_id: str, analyzer_id: str) -> list[str]:
    project_path = self._get_project_path(project_id)
    try:
      analyzers = os.listdir(os.path.join(
        project_path, "analyzers", analyzer_id, "secondary_outputs"))
      return analyzers
    except FileNotFoundError:
      return []

  def _ensure_database_version(self):
    """
    Makes sure that we have the correct database version

    This function will in future be extended to include any migration necessary
    to allow interoperability between versions.
    """
    q = Query()
    if self.db.search(q["class_"] == "version"):
      stored_version = self.db.search(
        q["class_"] == "version")[0]["version"]
      if stored_version != STORAGE_VERSION:
        raise Exception(
          f"Storage version mismatch: expected {
            STORAGE_VERSION}, got {stored_version}"
        )
    self.db.remove(q["class_"] == "version")
    self.db.insert({"class_": "version", "version": STORAGE_VERSION})

  def _find_unique_project_id(self, display_name: str):
    """Turn the display name into a unique project ID"""
    return self._get_unique_name(self._slugify_name(display_name), self._is_project_id_unique)

  def _is_project_id_unique(self, project_id: str):
    """Check the database if the project ID is unique"""
    q = Query()
    id_unique = not self.db.search(
      q["class_"] == "project" and q["id"] == project_id
    )
    dir_unique = not os.path.exists(self._get_project_path(project_id))
    return id_unique and dir_unique

  def _get_db_path(self):
    return os.path.join(self.user_data_dir, "db.json")

  def _get_project_path(self, project_id: str):
    return os.path.join(self.user_data_dir, "projects", project_id)

  def _get_project_input_path(self, project_id: str):
    return os.path.join(self._get_project_path(project_id), "input.parquet")

  def _get_project_primary_output_root_path(self, project_id: str, analyzer_id: str):
    return os.path.join(self._get_project_path(project_id), "analyzers", analyzer_id, "primary_outputs")

  def _get_project_secondary_output_root_path(self, project_id: str, analyzer_id: str, secondary_id: str):
    return os.path.join(self._get_project_path(project_id), "analyzers", analyzer_id, "secondary_outputs", secondary_id)

  def _get_project_exports_root_path(self, project_id: str, analyzer_id: str):
    return os.path.join(self._get_project_path(project_id), "analyzers", analyzer_id, "exports")

  def _get_web_presenter_state_path(self, project_id: str, analyzer_id: str, presenter_id: str):
    return os.path.join(self._get_project_path(project_id), "analyzers", analyzer_id, "web_presenters", presenter_id, "state")

  def _lock_database(self):
    """
    Locks the database to prevent concurrent access, in case multiple instances
    of the application are running.
    """
    lock_path = os.path.join(self.temp_dir, "db.lock")
    return FileLock(lock_path)

  def get_settings(self):
    with self._lock_database():
      return self._get_settings()

  def _get_settings(self):
    q = Query()
    settings = self.db.search(q["class_"] == "settings")
    if settings:
      return Settings(**settings[0])
    return Settings()

  def save_settings(self, **kwargs):
    with self._lock_database():
      q = Query()
      settings = self._get_settings()
      new_settings = Settings(**{
        **settings.model_dump(),
        **kwargs,
      })
      self.db.upsert(new_settings.model_dump(), q["class_"] == "settings")

  @staticmethod
  def _slugify_name(name: str):
    return re.sub(r'\W+', '_', name.lower()).strip("_")

  @staticmethod
  def _get_unique_name(base_name: str, validator: Callable[[str], bool]):
    if validator(base_name):
      return base_name
    i = 1
    while True:
      candidate = f"{base_name}_{i}"
      if validator(candidate):
        return candidate
      i += 1


class TableStats(BaseModel):
  num_rows: int


def collect_dataframe_chunks(input: Iterable[pl.DataFrame], size_threshold: int) -> Iterable[pl.DataFrame]:
  output_buffer = []
  size = 0
  for df in input:
    while True:
      available_space = size_threshold - size
      slice = df.head(available_space)
      output_buffer.append(slice)
      size = size + slice.height
      remaining_space = available_space - slice.height

      if remaining_space == 0:
        yield pl.concat(output_buffer)
        output_buffer = []
        size = 0

      if slice.height == df.height:
        break
      else:
        df = df.tail(-available_space)

  if output_buffer:
    yield pl.concat(output_buffer)
