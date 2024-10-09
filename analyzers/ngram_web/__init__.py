from analyzer_interface import WebPresenterDeclaration, WebPresenterInterface
from .factory import factory
from .interface import interface

ngrams_web = WebPresenterDeclaration(
  interface=interface,
  factory=factory,
  name=__name__
)

ngrams_web2 = WebPresenterDeclaration(
  interface=WebPresenterInterface(
      **{**interface.model_dump(), 'id': 'ngrams_web2', 'name': 'Another Dashboard'}),
  factory=factory,
  name=__name__
)
