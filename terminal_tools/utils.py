import sys
import os
import subprocess


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


def open_directory_explorer(path: str):
  if os.name == 'nt':
    # Windows platform
    subprocess.run(["explorer", os.path.normpath(path)])
  elif os.name == 'posix':
    if sys.platform == 'darwin':
      # macOS
      subprocess.run(["open", path])
    elif sys.platform == 'linux':
      if is_wsl():
        # WSL2 environment
        windows_path = subprocess.run(
          ["wslpath", "-w", path], capture_output=True, text=True).stdout.strip()
        subprocess.run(["explorer.exe", windows_path])
      else:
        # Native Linux
        subprocess.run(["xdg-open", path])
    else:
      raise OSError(f"Unsupported POSIX platform: {sys.platform}")
  else:
    raise OSError(f"Unsupported operating system: {os.name}")


def is_wsl() -> bool:
  """Check if the environment is WSL2."""
  try:
    with open('/proc/version', 'r') as f:
      return 'microsoft' in f.read().lower()
  except FileNotFoundError:
    return False


def print_ascii_table(rows: list[list[str]], *, header: list[str], min_widths: list[int] = []):
  # Determine the max number of columns
  max_columns = max([len(header), *(len(row) for row in rows)])

  # Make the data/header/min widths all the same column count
  def fill_row(row: list[str]):
    return [*row, *([""] * (max_columns - len(row)))]
  rows = list(fill_row(row) for row in rows)
  header = fill_row(header)
  min_widths = [*min_widths, *([0] * (max_columns - len(min_widths)))]

  # Determine the width of each column by finding the longest item in each column
  col_widths = [
    max([*(len(str(item)) for item in col), min_widths[i]])
    for i, col in enumerate(zip(*[header, *rows]))
  ]

  # Print the header
  header_row = (
    "│ " +
    " ┆ ".join(
      f"{header[i]:<{col_widths[i]}}" for i, _ in enumerate(header)) +
    " │"
  )

  def border_row(left: str, middle: str, right: str, char: str = "─"):
    return left + middle.join(char * w for w in col_widths) + right

  # top border
  print(border_row("┌─", "─┬─", "─┐"))

  print(header_row)

  # separator
  print(border_row("╞═", "═╪═", "═╡", "═"))

  # Print each row of data
  for row in rows:
    print(
      "│ " +
      " ┆ ".join(
        f"{str(row[i]):<{col_widths[i]}}" for i, _ in enumerate(header)) +
      " │"
    )

  # bottom border
  print(border_row("└─", "─┴─", "─┘"))
