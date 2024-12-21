from functools import cached_property

from pydantic import BaseModel, ConfigDict

from analyzer_interface.suite import AnalyzerSuite
from storage import Storage


class AppContext(BaseModel):
    storage: Storage
    suite: AnalyzerSuite
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @cached_property
    def settings(self):
        from .settings_context import SettingsContext

        return SettingsContext(app_context=self)
