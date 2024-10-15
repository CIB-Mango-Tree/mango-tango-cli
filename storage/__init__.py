import os
import re
from typing import Callable, Literal

import platformdirs
import polars as pl
from filelock import FileLock
from pydantic import BaseModel, Field
from tinydb import Query, TinyDB

STORAGE_VERSION = 1


class Project(BaseModel):
  class_: Literal["project"] = "project"
  id: str
  display_name: str


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

  def init_project(self, *, display_name: str, input: pl.DataFrame):
    with self._lock_database():
      project_id = self._find_unique_project_id(display_name)
      project = Project(id=project_id, display_name=display_name)
      self.db.insert(project.model_dump())

    project_dir = self._get_project_path(project_id)
    os.makedirs(project_dir, exist_ok=True)

    input_path = self._get_project_input_path(project_id)
    input.write_parquet(input_path)
    return project

  def list_projects(self):
    q = Query()
    projects = self.db.search(q["class_"] == "project")
    return sorted(
      (Project(**project) for project in projects),
      key=lambda project: project.display_name
    )

  def load_project_input(self, project_id: str):
    input_path = self._get_project_input_path(project_id)
    return pl.read_parquet(input_path)

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

  def _save_output(self, output_path_without_extension, output_df: pl.DataFrame, extension: SupportedOutputExtension,):
    os.makedirs(os.path.dirname(output_path_without_extension), exist_ok=True)
    output_path = f"{output_path_without_extension}.{extension}"
    if extension == "parquet":
      output_df.write_parquet(output_path)
    elif extension == "csv":
      output_df.write_csv(output_path)
    elif extension == "xlsx":
      output_df.write_excel(output_path)
    elif extension == "json":
      output_df.write_json(output_path)
    else:
      raise ValueError(f"Unsupported format: {extension}")
    return output_path

  def load_project_primary_output(self, project_id: str, analyzer_id: str, output_id: str):
    output_path = os.path.join(self._get_project_primary_output_root_path(
      project_id, analyzer_id), f"{output_id}.parquet")
    return pl.read_parquet(output_path)

  def load_project_secondary_output(self, project_id: str, analyzer_id: str, secondary_id: str, output_id: str):
    output_path = os.path.join(self._get_project_secondary_output_root_path(
      project_id, analyzer_id, secondary_id), f"{output_id}.parquet")
    return pl.read_parquet(output_path)

  def export_project_primary_output(self, project_id: str, analyzer_id: str, output_id: str, extension: SupportedOutputExtension):
    output_df = self.load_project_primary_output(
      project_id, analyzer_id, output_id)
    output_path = os.path.join(
      self._get_project_exports_root_path(project_id, analyzer_id),
      output_id
    )
    return self._save_output(output_path, output_df, extension)

  def export_project_secondary_output(self, project_id: str, analyzer_id: str, secondary_id: str, output_id: str, extension: SupportedOutputExtension):
    output_df = self.load_project_secondary_output(
      project_id, analyzer_id, secondary_id, output_id)
    output_path = os.path.join(
      self._get_project_exports_root_path(project_id, analyzer_id),
      (secondary_id if secondary_id ==
       output_id else f"{secondary_id}__{output_id}")
    )
    return self._save_output(output_path, output_df, extension)

  def list_project_analyses(self, project_id: str):
    project_path = self._get_project_path(project_id)
    try:
      analyzers = os.listdir(os.path.join(project_path, "analyzers"))
      return analyzers
    except FileNotFoundError:
      return []

  def list_project_secondary_analyses(self, project_id: str, analyzer_id: str):
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
    return not self.db.search(q["class_"] == "project" and q["id"] == project_id)

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

  def _lock_database(self):
    """
    Locks the database to prevent concurrent access, in case multiple instances
    of the application are running.
    """
    lock_path = os.path.join(self.temp_dir, "db.lock")
    return FileLock(lock_path)

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
