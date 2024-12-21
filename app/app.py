from tempfile import NamedTemporaryFile

from pydantic import BaseModel

from importing import ImporterSession

from .app_context import AppContext
from .project_context import ProjectContext


class App(BaseModel):
    context: AppContext

    def list_projects(self):
        return [
            ProjectContext(model=project, app_context=self.context)
            for project in self.context.storage.list_projects()
        ]

    def create_project(self, name: str, importer_session: ImporterSession):
        with NamedTemporaryFile(delete=False) as temp_file:
            importer_session.import_as_parquet(temp_file.name)
        project_model = self.context.storage.init_project(
            display_name=name, input_temp_file=temp_file.name
        )
        return ProjectContext(model=project_model, app_context=self.context)

    @property
    def file_selector_state(self):
        return self.context.storage.file_selector_state
