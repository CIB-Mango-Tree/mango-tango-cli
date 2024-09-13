import sys
import os


def clear_terminal():
  """Clears the terminal
  """
  if os.name == "nt":
    os.system("cls")
  else:
    os.system("clear")


def wait_for_key(prompt: bool = False):
  """Waits for the user to press any key

  Args:
      prompt (bool, optional): If true, a default text
      `Press any key to continue` will be shown. Defaults to False.
  """
  if prompt:
    print("Press any key to continue...", end="", flush=True)
  _wait_for_key()


if os.name == 'nt':
  import msvcrt

  def _wait_for_key():
    return msvcrt.getch()
else:
  import tty
  import termios

  def _wait_for_key():
    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
      tty.setraw(fd)
      ch = sys.stdin.read(1)
    finally:
      termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    return ch


def enable_windows_ansi_support():
  """
  Set up the Windows terminal to support ANSI escape codes, which will be needed
  for colored text, line clearing, and other terminal features.
  """
  if os.name == 'nt':
    # Enable ANSI escape code support for Windows
    # On Windows, calling os.system('') with an empty string doesn't
    # run any actual command. However, there's an undocumented side
    # effect: it forces the Windows terminal to initialize or refresh
    # its state, enabling certain features like the processing of ANSI
    # escape codes, which might not otherwise be active.
    os.system('')


def clear_printed_lines(count: int):
  """
  Clear the last `count` lines of the terminal. Useful for repainting
  terminal output.

  Args:
      count (int): The number of lines to clear
  """
  for _ in range(count + 1):
    sys.stdout.write('\033[2K')  # Clear the current line
    sys.stdout.write('\033[F')   # Move cursor up one line
  sys.stdout.write('\033[2K\r')    # Clear the last line and move to start
  sys.stdout.flush()


def draw_box(text: str, *, padding_spaces: int = 5, padding_lines: int = 1) -> str:
  """
  Draw a box around the given text, which will be centered in the box.

  Args:
      text (str): The text to be drawn, may be multiline.
        ANSI formatting and emojis are not supported, as they mess with
        both the character count calculation and the monospace font.

      padding_spaces (int, optional): Extra spaces on either side of the longest line. Defaults to 5.
      padding_lines (int, optional): Extra lines above and below the text. Defaults to 1.

  Returns:
      str: The text surrounded by a box.
  """
  lines = text.split("\n")
  width = max(len(line) for line in lines) + padding_spaces * 2

  box = ""
  box += "┌" + "─" * width + "┐\n"
  for _ in range(padding_lines):
    box += "│" + " " * width + "│\n"
  for line in lines:
    padding = " " * padding_spaces
    box += "│" + padding + \
        line.center(width - 2 * padding_spaces) + padding + "│\n"
  for _ in range(padding_lines):
    box += "│" + " " * width + "│\n"
  box += "└" + "─" * width + "┘\n"
  return box
