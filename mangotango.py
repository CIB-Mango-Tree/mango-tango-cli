from components import main_menu, splash
from terminal_tools import enable_windows_ansi_support
from terminal_tools.inception import Context
import warnings


if __name__ == "__main__":
  warnings.simplefilter(action='ignore', category=UserWarning)
  enable_windows_ansi_support()
  splash()
  main_menu(Context())
