import os

from typing import Optional
from inquirer import (
  confirm as inquirer_confirm,
  list_input as inquirer_list_input,
  text as inquirer_text, checkbox as inquirer_checkbox,
)
from inquirer.errors import ValidationError
from storage.file_selector import FileSelectorStateManager

from .utils import clear_printed_lines

if os.name == "nt":
  from ctypes import windll
  from string import ascii_uppercase

def get_drives():
  
  """
  Returns a list of the logically assigned drives on a windows system.
  
  Args:
      None
      
  Returns:
      list: A list of drive letters available and accessible on the system.
  """

  drives = []
  bitmask = windll.kernel32.GetLogicalDrives()
  
  for letter in ascii_uppercase:
      if bitmask & 1:
          drives.append(letter+":")
      bitmask >>= 1

  return drives


def file_selector(
  message: str = "select a file", *,
  state: Optional[FileSelectorStateManager] = None
):
  """Lets the user select a file from the filesystem.

  Args:
      message (str, optional): The prompt message. Defaults to "select a file".
      initial_path (str, optional): Where to start the directory listing.
        Defaults to current working directory.

  Returns:
      (str, optional): The absolute path selected by the user, or None if the
        user cancels the prompt.
  """
  initial_dir = state and state.get_current_path()
  if not os.path.isdir(initial_dir):
    initial_dir = None

  current_path = os.path.realpath(initial_dir or os.curdir)

  if os.name == "nt":
    drives = get_drives()
    drive_choices = [(drive, drive) for drive in drives]

  def is_dir(entry: str):
    return os.path.isdir(os.path.join(current_path, entry))

  while True:
    print(f"current path: {current_path}")
    choices = [
        ("[..]", ".."),
        *(
            (f"[{entry}]" if is_dir(entry) else entry, entry)
            for entry in sorted(os.listdir(current_path))
        ),
    ]
    
    # Add change drive option to the list of choices if on Windows
    if os.name == "nt":
      cur_drive = os.path.splitdrive(current_path)[0]
      choices.insert(0, (f"[Change Drive (current - {cur_drive})]", 'change_drive'))

    selected_entry = list_input(message, choices=choices)
    
    if selected_entry is not None and selected_entry == 'change_drive':
      selected_drive = list_input("Select a drive:", choices=drive_choices)
      if selected_drive is None:
        return None
      
      current_path = selected_entry = f"{selected_drive}\\"
      # clear the prompted lines
      clear_printed_lines(len(drives)+1)

    # inquirer will show up to 14 lines including the header
    # we have one line for the current path to rewrite
    clear_printed_lines(min(len(choices), 13) + 2)

    if selected_entry is None:
      return None

    if is_dir(selected_entry):
      current_path = os.path.realpath(
        os.path.join(current_path, selected_entry))
    else:
      if state is not None:
        state.set_current_path(current_path)
      return os.path.join(current_path, selected_entry)


def list_input(message: str, **kwargs):
  """
  Wraps `inquirer`'s list input and catches KeyboardInterrupt
  """
  return wrap_keyboard_interrupt(lambda: inquirer_list_input(message, **kwargs))


def checkbox(message: str, **kwargs):
  """
  Wraps `inquirer`'s checkbox and catches KeyboardInterrupt
  """
  return wrap_keyboard_interrupt(lambda: inquirer_checkbox(message, **kwargs))


def confirm(message: str, *, cancel_fallback: Optional[bool] = False, **kwargs):
  """
  Wraps `inquirer`'s confirm input and catches KeyboardInterrupt
  """
  return wrap_keyboard_interrupt(lambda: inquirer_confirm(message, **kwargs), cancel_fallback)


def text(message: str, **kwargs):
  """
  Wraps `inquirer`'s text input and catches KeyboardInterrupt
  """
  return wrap_keyboard_interrupt(lambda: inquirer_text(message, **kwargs))


def int_input(
  message: str, *,
  min: Optional[int] = None,
  max: Optional[int] = None,
  **kwargs) -> Optional[int]:
  """
  Wraps `inquirer`'s text input and catches KeyboardInterrupt
  """
  def validate_value(value):
    try:
      value = int(value)
    except ValueError:
      raise ValidationError("Please enter a valid integer.")

    if min is not None and value < min:
      raise ValidationError(
        f"Please enter a value greater than or equal to {min}.")

    if max is not None and value > max:
      raise ValidationError(
        f"Please enter a value less than or equal to {max}.")

    return True

  return wrap_keyboard_interrupt(
    lambda: inquirer_text(
      message,
      validate=lambda previous_answers, value: validate_value(value), **kwargs
    ),
    None
  )


def wrap_keyboard_interrupt(fn, fallback=None):
  """
  Calls `fn` and catches KeyboardInterrupt, returning `fallback` if it occurs.
  """
  try:
    return fn()
  except KeyboardInterrupt:
    return fallback
