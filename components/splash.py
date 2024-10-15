from terminal_tools import clear_terminal, wait_for_key
from pathlib import Path
import os


def splash():
  clear_terminal()
  print(_ascii_splash)
  print("")
  print(f"{get_version()}")
  print("")
  wait_for_key(True)


def get_version():
  root_path = str(Path(__file__).resolve().parent.parent)
  version_path = os.path.join(root_path, "VERSION")
  try:
    with open(version_path, "r") as version_file:
      return version_file.read().strip()
  except FileNotFoundError:
    return "<development version>"
  except PermissionError:  # Swallow this for now
    return ""


_ascii_splash: str = """
       -..*+:..-
       -.=-+%@%##+-=.-
    = =:*%:...=:..=@*:+ =
  :: -:=#==#*=:::-=-...-:::
 =.*++:%#*##=##+++:.*%*++..=
 @@@::--#@%#%%###%#@#-:::@@@
 ..:-##%@#@#%%%%++@#@%#+-=...
@@@@#-%@@#+#+++##+*+@@%%#@@@%
  : %#     @# %*++    :#% :
            @##%
            @@#
            @@#=
            @@%
            @@@

 C I B   M A N G O   T R E E

   A Civic Tech DC Project

       ╱ * * *  ╱ ╲
       ╲ ===== ╱  ╱
"""
'''
I generated this using https://www.asciiart.eu/image-to-ascii
'''
