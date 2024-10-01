from components import main_menu, splash
from terminal_tools import enable_windows_ansi_support
from terminal_tools.inception import TerminalContext
from storage import Storage

if __name__ == "__main__":
  enable_windows_ansi_support()
  storage = Storage(app_name="MangoTango", app_author="Civic Tech DC")

  splash()
  main_menu(TerminalContext(), storage)
