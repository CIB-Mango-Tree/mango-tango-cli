from multiprocessing import freeze_support

from analyzers import suite
from components import main_menu, splash
from storage import Storage
from terminal_tools import enable_windows_ansi_support
from terminal_tools.inception import TerminalContext

if __name__ == "__main__":
    freeze_support()
    enable_windows_ansi_support()
    storage = Storage(app_name="MangoTango", app_author="Civic Tech DC")

    splash()
    main_menu(TerminalContext(), storage, suite)
