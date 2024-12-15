from abc import ABC, abstractmethod
from typing import Optional, TypeVar, Callable

import polars as pl


class ImporterSession(ABC):
  """
  The ImporterSession interface handles the ongoing configuration of an import.
  It keeps the configuration state, knows how to print the configuration to the
  console, and can load a preview of the data from the input file.
  """

  @abstractmethod
  def print_config(self) -> None:
    """
    Print the configuration of the import session to the console.
    """
    pass

  @abstractmethod
  def load_preview(self, n_records: int) -> Optional[pl.DataFrame]:
    """
    Attempt to load a preview of the data from the input file.

    Return None here if it is sure that the file cannot be loaded with the current
    configuration. Only throw an execption in the case of unexpected errors.
    """
    pass

  @abstractmethod
  def import_as_parquet(self, output_path: str) -> None:
    """
    Import the data from the input file to the output file in the Parquet format.
    """
    pass


SessionType = TypeVar("SessionType", bound=ImporterSession)


class Importer[SessionType](ABC):
  @property
  @abstractmethod
  def name(self) -> str:
    """
    The name of the importer. It will be quoted in the UI in texts such as
    "Imported as `name`, so keep it to a format name."
    """
    pass

  @abstractmethod
  def suggest(self, input_path: str) -> bool:
    """
    Check if the importer can handle the given file. This should be fairly
    restrictive based on reasonable assumptions, as it is only used for the
    initial importer suggestion. The user can always override the suggestion.
    """
    pass

  @abstractmethod
  def init_session(self, input_path: str) -> Optional[SessionType]:
    """
    Produces an initial import session object that contains all the configuration
    needed for the import. The user can either accept this configuration or
    customize it.

    Return None here if the importer cannot figure out how to configure the
    import parameters. This doesn't necessarily mean that the file cannot be
    loaded; the UI will force the user to customize the import session if the
    user wants to proceed with this importer.
    """
    pass

  @abstractmethod
  def manual_init_session(self, input_path: str) -> Optional[SessionType]:
    pass

  @abstractmethod
  def modify_session(self, input_path: str, import_session: SessionType, reset_screen: Callable[[SessionType], None]) -> Optional[SessionType]:
    """
    Performs the interactive UI sequence that customizes the import session
    from the initial one.

    Return None here if the user interrupts the customization process.
    """
    pass
