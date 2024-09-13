"""
The inception module aids in creating nested terminal blocks (hence the name
"inception"). It provides a `Context` class that manages a list of `Scope`
instances. Each `Scope` instance represents a block of text that are buffered
in memory and printed to the terminal at each refresh.
"""


from .utils import clear_terminal


class Context:
  def __init__(self):
    self.scopes: list[Scope] = []

  def nest(self, text: str):
    scope = Scope(context=self, text=text)
    return scope

  def _append_scope(self, block: "Scope"):
    self.scopes.append(block)

  def _remove_scope(self, block: "Scope"):
    self.scopes.remove(block)

  def _refresh(self):
    clear_terminal()
    for scope in self.scopes:
      scope.print()


class Scope:
  def __init__(self, context: Context, text: str):
    self.context = context
    self.text = text

  def print(self):
    print(self.text)

  def __enter__(self):
    self.context._append_scope(self)
    self.context._refresh()

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.context._remove_scope(self)
