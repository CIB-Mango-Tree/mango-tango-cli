from components import main_menu, splash
from terminal_tools import enable_windows_ansi_support
from terminal_tools.inception import Context


if __name__ == "__main__":
  enable_windows_ansi_support()
  splash()
  main_menu(Context())
