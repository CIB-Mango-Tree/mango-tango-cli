from typing import Callable

from .context import (
    PrimaryAnalyzerContext,
    SecondaryAnalyzerContext,
    WebPresenterContext,
)
from .interface import (
    AnalyzerInterface,
    SecondaryAnalyzerInterface,
    WebPresenterInterface,
)


class AnalyzerDeclaration(AnalyzerInterface):
    entry_point: Callable[[PrimaryAnalyzerContext], None]
    is_distributed: bool

    def __init__(
        self,
        interface: AnalyzerInterface,
        main: Callable,
        *,
        is_distributed: bool = False
    ):
        """Creates a primary analyzer declaration

        Args:
          interface (AnalyzerInterface): The metadata interface for the primary analyzer.

          main (Callable):
            The entry point function for the primary analyzer. This function should
            take a single argument of type `PrimaryAnalyzerContext` and should ensure
            that the outputs specified in the interface are generated.

          is_distributed (bool):
            Set this explicitly to `True` once the analyzer is ready to be shipped
            to end users; it will make the analyzer available in the distributed
            executable.
        """
        super().__init__(
            **interface.model_dump(), entry_point=main, is_distributed=is_distributed
        )


class SecondaryAnalyzerDeclaration(SecondaryAnalyzerInterface):
    entry_point: Callable[["SecondaryAnalyzerContext"], None]

    def __init__(self, interface: SecondaryAnalyzerInterface, main: Callable):
        """Creates a secondary analyzer declaration

        Args:
          interface (SecondaryAnalyzerInterface): The metadata interface for the secondary analyzer.

          main (Callable):
            The entry point function for the secondary analyzer. This function should
            take a single argument of type `SecondaryAnalyzerContext` and should ensure
            that the outputs specified in the interface are generated.
        """
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
