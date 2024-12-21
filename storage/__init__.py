import math
import os
import re
import shutil
from datetime import datetime
from typing import Callable, Iterable, Literal, Optional

import platformdirs
import polars as pl
import pyarrow.parquet as pq
from filelock import FileLock
from pydantic import BaseModel
from tinydb import Query, TinyDB
from xlsxwriter import Workbook

from analyzer_interface.interface import AnalyzerOutput

from .file_selector import FileSelectorStateManager


class ProjectModel(BaseModel):
    class_: Literal["project"] = "project"
    id: str
    display_name: str


class SettingsModel(BaseModel):
    class_: Literal["settings"] = "settings"
    export_chunk_size: Optional[int | Literal[False]] = None


class FileSelectionState(BaseModel):
    class_: Literal["file_selector_state"] = "file_selector_state"
    last_path: Optional[str] = None


class AnalysisModel(BaseModel):
    class_: Literal["analysis"] = "analysis"
    analysis_id: str
    project_id: str
    display_name: str
    primary_analyzer_id: str
    path: str
    column_mapping: Optional[dict[str, str]] = None
    create_timestamp: Optional[float] = None
    is_draft: bool = False

    def create_time(self):
        return (
            datetime.fromtimestamp(self.create_timestamp)
            if self.create_timestamp
            else None
        )


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
            self._bootstrap_analyses_v1()

        self.file_selector_state = AppFileSelectorStateManager(self)

    def init_project(self, *, display_name: str, input_temp_file: str):
        with self._lock_database():
            project_id = self._find_unique_project_id(display_name)
            project = ProjectModel(id=project_id, display_name=display_name)
            self.db.insert(project.model_dump())

        project_dir = self._get_project_path(project_id)
        os.makedirs(project_dir, exist_ok=True)

        shutil.move(input_temp_file, self._get_project_input_path(project_id))
        return project

    def list_projects(self):
        q = Query()
        projects = self.db.search(q["class_"] == "project")
        return sorted(
            (ProjectModel(**project) for project in projects),
            key=lambda project: project.display_name,
        )

    def get_project(self, project_id: str):
        q = Query()
        project = self.db.search((q["class_"] == "project") & (q["id"] == project_id))
        if project:
            return ProjectModel(**project[0])
        return None

    def delete_project(self, project_id: str):
        with self._lock_database():
            q = Query()
            self.db.remove((q["id"] == project_id) & (q["class_"] == "project"))
        project_path = self._get_project_path(project_id)
        shutil.rmtree(project_path, ignore_errors=True)

    def rename_project(self, project_id: str, name: str):
        with self._lock_database():
            q = Query()
            self.db.update(
                {"display_name": name},
                (q["id"] == project_id) & (q["class_"] == "project"),
            )

    def load_project_input(self, project_id: str, *, n_records: Optional[int] = None):
        input_path = self._get_project_input_path(project_id)
        return pl.read_parquet(input_path, n_rows=n_records)

    def get_project_input_stats(self, project_id: str):
        input_path = self._get_project_input_path(project_id)
        num_rows = pl.scan_parquet(input_path).select(pl.count()).collect().item()
        return TableStats(num_rows=num_rows)

    def save_project_primary_outputs(
        self, analysis: AnalysisModel, outputs: dict[str, pl.DataFrame]
    ):
        for output_id, output_df in outputs.items():
            self._save_output(
                os.path.join(
                    self._get_project_primary_output_root_path(analysis),
                    output_id,
                ),
                output_df,
                "parquet",
            )

    def save_project_secondary_outputs(
        self,
        analysis: AnalysisModel,
        secondary_id: str,
        outputs: dict[str, pl.DataFrame],
    ):
        for output_id, output_df in outputs.items():
            self._save_output(
                os.path.join(
                    self._get_project_secondary_output_root_path(
                        analysis, secondary_id
                    ),
                    output_id,
                ),
                output_df,
                "parquet",
            )

    def save_project_secondary_output(
        self,
        analysis: AnalysisModel,
        secondary_id: str,
        output_id: str,
        output_df: pl.DataFrame,
        extension: SupportedOutputExtension,
    ):
        root_path = self._get_project_secondary_output_root_path(analysis, secondary_id)
        self._save_output(
            os.path.join(root_path, output_id),
            output_df,
            extension,
        )

    def _save_output(
        self,
        output_path_without_extension,
        output_df: pl.DataFrame | pl.LazyFrame,
        extension: SupportedOutputExtension,
    ):
        output_df = output_df.lazy()
        os.makedirs(os.path.dirname(output_path_without_extension), exist_ok=True)
        output_path = f"{output_path_without_extension}.{extension}"
        if extension == "parquet":
            output_df.sink_parquet(output_path)
        elif extension == "csv":
            output_df.sink_csv(output_path)
        elif extension == "xlsx":
            # See https://xlsxwriter.readthedocs.io/working_with_dates_and_time.html#timezone-handling
            with Workbook(output_path, {"remove_timezone": True}) as workbook:
                output_df.collect().write_excel(workbook)
        elif extension == "json":
            output_df.collect().write_json(output_path)
        else:
            raise ValueError(f"Unsupported format: {extension}")
        return output_path

    def load_project_primary_output(self, analysis: AnalysisModel, output_id: str):
        output_path = self.get_primary_output_parquet_path(analysis, output_id)
        return pl.read_parquet(output_path)

    def get_primary_output_parquet_path(self, analysis: AnalysisModel, output_id: str):
        return os.path.join(
            self._get_project_primary_output_root_path(analysis),
            f"{output_id}.parquet",
        )

    def load_project_secondary_output(
        self, analysis: AnalysisModel, secondary_id: str, output_id: str
    ):
        output_path = self.get_secondary_output_parquet_path(
            analysis, secondary_id, output_id
        )
        return pl.read_parquet(output_path)

    def get_secondary_output_parquet_path(
        self, analysis: AnalysisModel, secondary_id: str, output_id: str
    ):
        return os.path.join(
            self._get_project_secondary_output_root_path(analysis, secondary_id),
            f"{output_id}.parquet",
        )

    def export_project_primary_output(
        self,
        analysis: AnalysisModel,
        output_id: str,
        *,
        extension: SupportedOutputExtension,
        spec: AnalyzerOutput,
        export_chunk_size: Optional[int] = None,
    ):
        return self._export_output(
            self.get_primary_output_parquet_path(analysis, output_id),
            os.path.join(self._get_project_exports_root_path(analysis), output_id),
            extension=extension,
            spec=spec,
            export_chunk_size=export_chunk_size,
        )

    def export_project_secondary_output(
        self,
        analysis: AnalysisModel,
        secondary_id: str,
        output_id: str,
        *,
        extension: SupportedOutputExtension,
        spec: AnalyzerOutput,
        export_chunk_size: Optional[int] = None,
    ):
        exported_path = os.path.join(
            self._get_project_exports_root_path(analysis),
            (
                secondary_id
                if secondary_id == output_id
                else f"{secondary_id}__{output_id}"
            ),
        )
        return self._export_output(
            self.get_secondary_output_parquet_path(analysis, secondary_id, output_id),
            exported_path,
            extension=extension,
            spec=spec,
            export_chunk_size=export_chunk_size,
        )

    def _export_output(
        self,
        input_path: str,
        output_path: str,
        *,
        extension: SupportedOutputExtension,
        spec: AnalyzerOutput,
        export_chunk_size: Optional[int] = None,
    ):
        with pq.ParquetFile(input_path) as reader:
            num_chunks = (
                math.ceil(reader.metadata.num_rows / export_chunk_size)
                if export_chunk_size
                else 1
            )

        if num_chunks == 1:
            df = pl.scan_parquet(input_path)
            self._save_output(output_path, spec.transform_output(df), extension)
            return f"{output_path}.{extension}"

        with pq.ParquetFile(input_path) as reader:
            get_batches = (
                df
                for batch in reader.iter_batches()
                if (df := pl.from_arrow(batch)) is not None
            )
            for chunk_id, chunk in enumerate(
                collect_dataframe_chunks(get_batches, export_chunk_size)
            ):
                chunk = spec.transform_output(chunk)
                self._save_output(f"{output_path}_{chunk_id}", chunk, extension)
                yield chunk_id / num_chunks
            return f"{output_path}_[*].{extension}"

    def list_project_analyses(self, project_id: str):
        with self._lock_database():
            q = Query()
            analysis_models = self.db.search(
                (q["class_"] == "analysis") & (q["project_id"] == project_id)
            )
        return [AnalysisModel(**analysis) for analysis in analysis_models]

    def init_analysis(
        self,
        project_id: str,
        display_name: str,
        primary_analyzer_id: str,
        column_mapping: dict[str, str],
    ) -> AnalysisModel:
        with self._lock_database():
            analysis_id = self._find_unique_analysis_id(project_id, display_name)
            analysis = AnalysisModel(
                analysis_id=analysis_id,
                project_id=project_id,
                display_name=display_name,
                primary_analyzer_id=primary_analyzer_id,
                path=os.path.join("analysis", analysis_id),
                column_mapping=column_mapping,
                create_timestamp=datetime.now().timestamp(),
                is_draft=True,
            )
            self.db.insert(analysis.model_dump())
        return analysis

    def save_analysis(self, analysis: AnalysisModel):
        with self._lock_database():
            q = Query()
            self.db.update(
                analysis.model_dump(),
                (q["class_"] == "analysis")
                & (q["project_id"] == analysis.project_id)
                & (q["analysis_id"] == analysis.analysis_id),
            )

    def delete_analysis(self, analysis: AnalysisModel):
        with self._lock_database():
            q = Query()
            self.db.remove(
                (q["class_"] == "analysis")
                & (q["project_id"] == analysis.project_id)
                & (q["analysis_id"] == analysis.analysis_id)
            )
            analysis_path = os.path.join(
                self._get_project_path(analysis.project_id), analysis.path
            )
            shutil.rmtree(analysis_path, ignore_errors=True)

    def _find_unique_analysis_id(self, project_id: str, display_name: str):
        return self._get_unique_name(
            self._slugify_name(display_name),
            lambda analysis_id: self._is_analysis_id_unique(project_id, analysis_id),
        )

    def _is_analysis_id_unique(self, project_id: str, analysis_id: str):
        q = Query()
        id_unique = not self.db.search(
            (q["class_"] == "analysis")
            & (q["project_id"] == project_id)
            & (q["analysis_id"] == analysis_id)
        )
        dir_unique = not os.path.exists(
            os.path.join(self._get_project_path(project_id), "analysis", analysis_id)
        )
        return id_unique and dir_unique

    def _bootstrap_analyses_v1(self):
        legacy_v1_analysis_dirname = "analyzers"
        projects = self.list_projects()
        for project in projects:
            project_id = project.id
            project_path = self._get_project_path(project_id)
            try:
                v1_analyses = os.listdir(
                    os.path.join(project_path, legacy_v1_analysis_dirname)
                )
            except FileNotFoundError:
                continue
            for analyzer_id in v1_analyses:
                db_analyzer_id = f"__v1__{analyzer_id}"
                modified_time = os.path.getmtime(
                    os.path.join(project_path, legacy_v1_analysis_dirname, analyzer_id)
                )
                self.db.upsert(
                    AnalysisModel(
                        analysis_id=db_analyzer_id,
                        project_id=project_id,
                        display_name=analyzer_id,
                        primary_analyzer_id=analyzer_id,
                        path=os.path.join(legacy_v1_analysis_dirname, analyzer_id),
                        create_timestamp=modified_time,
                    ).model_dump(),
                    (Query()["class_"] == "analysis")
                    & (Query()["project_id"] == project_id)
                    & (Query()["analysis_id"] == db_analyzer_id),
                )

    def list_secondary_analyses(self, analysis: AnalysisModel) -> list[str]:
        try:
            analyzers = os.listdir(
                os.path.join(
                    self._get_project_path(analysis.project_id),
                    analysis.path,
                    "secondary_outputs",
                ),
            )
            return analyzers
        except FileNotFoundError:
            return []

    def _find_unique_project_id(self, display_name: str):
        """Turn the display name into a unique project ID"""
        return self._get_unique_name(
            self._slugify_name(display_name), self._is_project_id_unique
        )

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

    def _get_project_primary_output_root_path(self, analysis: AnalysisModel):
        return os.path.join(
            self._get_project_path(analysis.project_id),
            analysis.path,
            "primary_outputs",
        )

    def _get_project_secondary_output_root_path(
        self, analysis: AnalysisModel, secondary_id: str
    ):
        return os.path.join(
            self._get_project_path(analysis.project_id),
            analysis.path,
            "secondary_outputs",
            secondary_id,
        )

    def _get_project_exports_root_path(self, analysis: AnalysisModel):
        return os.path.join(
            self._get_project_path(analysis.project_id), analysis.path, "exports"
        )

    def _get_web_presenter_state_path(self, analysis: AnalysisModel, presenter_id: str):
        return os.path.join(
            self._get_project_path(analysis.project_id),
            analysis.path,
            "web_presenters",
            presenter_id,
            "state",
        )

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
            return SettingsModel(**settings[0])
        return SettingsModel()

    def save_settings(self, **kwargs):
        with self._lock_database():
            q = Query()
            settings = self._get_settings()
            new_settings = SettingsModel(
                **{
                    **settings.model_dump(),
                    **{
                        key: value for key, value in kwargs.items() if value is not None
                    },
                }
            )
            self.db.upsert(new_settings.model_dump(), q["class_"] == "settings")

    @staticmethod
    def _slugify_name(name: str):
        return re.sub(r"\W+", "_", name.lower()).strip("_")

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


def collect_dataframe_chunks(
    input: Iterable[pl.DataFrame], size_threshold: int
) -> Iterable[pl.DataFrame]:
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


class AppFileSelectorStateManager(FileSelectorStateManager):
    def __init__(self, storage: "Storage"):
        self.storage = storage

    def get_current_path(self):
        return self._load_state().last_path

    def set_current_path(self, path: str):
        self._save_state(path)

    def _load_state(self):
        q = Query()
        state = self.storage.db.search(q["class_"] == "file_selector_state")
        if state:
            return FileSelectionState(**state[0])
        return FileSelectionState()

    def _save_state(self, last_path: str):
        self.storage.db.upsert(
            FileSelectionState(last_path=last_path).model_dump(),
            Query()["class_"] == "file_selector_state",
        )
