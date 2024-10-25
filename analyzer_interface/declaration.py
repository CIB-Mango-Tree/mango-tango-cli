from typing import Callable

from .context import (PrimaryAnalyzerContext, SecondaryAnalyzerContext,
                      WebPresenterContext)
from .interface import (AnalyzerInterface, SecondaryAnalyzerInterface,
                        WebPresenterInterface)


class AnalyzerDeclaration(AnalyzerInterface):
  """
  The analyzer's entry point. The function should ensure that the outputs
  specified in the interface are generated.
  """
  entry_point: Callable[[PrimaryAnalyzerContext], None]

  def __init__(self, interface: AnalyzerInterface, main: Callable):
    super().__init__(**interface.model_dump(), entry_point=main)


class SecondaryAnalyzerDeclaration(SecondaryAnalyzerInterface):
  entry_point: Callable[["SecondaryAnalyzerContext"], None]
  """
  The analyzer's entry point. The function should ensure that the outputs
  specified in the interface are generated.
  """

  def __init__(self, interface: SecondaryAnalyzerInterface, main: Callable):
    super().__init__(**interface.model_dump(), entry_point=main)


class WebPresenterDeclaration(WebPresenterInterface):
  factory: Callable[["WebPresenterContext"], None]

  server_name: str

  def __init__(self, interface: WebPresenterInterface, factory: Callable, name: str):
    """Creates a web presenter declaration

    Args:
      interface (WebPresenterInterface): The metadata interface for the web presenter.

      factory (Callable):
        The factory function that creates a Dash app for the web presenter. It should
        modify the Dash app in the context to add whatever plotting interface
        the web presenter needs.

      server_name (str):
        The server name for the Dash app. Typically, you will use the global
        variable `__name__` here.

        If your web presenter has assets like images, CSS or JavaScript files,
        you can put them in a folder named `assets` in the same directory
        as the file where `__name__` is used. The Dash app will serve these
        files at the `/assets/` URL, using the python module name in `__name__`
        to determine the absolute path to the assets folder.

        See Dash documentation for more details: https://dash.plotly.com
        See also Python documentation for the `__name__` variable:
        https://docs.python.org/3/tutorial/modules.html

    """
    super().__init__(**interface.model_dump(), factory=factory, server_name=name)
