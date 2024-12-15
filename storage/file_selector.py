from abc import ABC, abstractmethod
from typing import Optional


class FileSelectorStateManager(ABC):
  @abstractmethod
  def get_current_path(self) -> Optional[str]:
    pass

  @abstractmethod
  def set_current_path(self, path: str):
    pass
