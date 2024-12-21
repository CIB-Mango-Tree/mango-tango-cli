from typing import Literal

from pydantic import BaseModel

from storage import SettingsModel

from .app_context import AppContext


class SettingsContext(BaseModel):
    app_context: AppContext

    @property
    def export_chunk_size(self):
        return self.app_context.storage.get_settings().export_chunk_size

    def set_export_chunk_size(self, value: int | Literal[False]):
        self.app_context.storage.save_settings(
            **SettingsModel(export_chunk_size=value).model_dump()
        )
