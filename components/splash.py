from terminal_tools import clear_terminal, wait_for_key


def splash():
  clear_terminal()
  print(_ascii_splash)
  wait_for_key(True)


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
