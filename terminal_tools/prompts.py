import os

from typing import Optional
from inquirer import (
  confirm as inquirer_confirm,
  list_input as inquirer_list_input,
  text as inquirer_text, checkbox as inquirer_checkbox
)

from .utils import clear_printed_lines


def file_selector(
  message: str = "select a file", *,
  initial_path: str = os.curdir
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
  current_path = os.path.realpath(initial_path)

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
    selected_entry = list_input(message, choices=choices)

    # inquirer will show up to 14 lines including the header
    # we have one line for the current path to rewrite
    clear_printed_lines(min(len(choices), 13) + 2)

    if selected_entry is None:
      return None

    if is_dir(selected_entry):
      current_path = os.path.realpath(
        os.path.join(current_path, selected_entry))
    else:
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


def wrap_keyboard_interrupt(fn, fallback=None):
  """
  Calls `fn` and catches KeyboardInterrupt, returning `fallback` if it occurs.
  """
  try:
    return fn()
  except KeyboardInterrupt:
    return fallback
