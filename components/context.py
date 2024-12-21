from pydantic import BaseModel, ConfigDict

from app import App
from terminal_tools.inception import TerminalContext


class ViewContext(BaseModel):
    terminal: TerminalContext
    app: App
    model_config = ConfigDict(arbitrary_types_allowed=True)
