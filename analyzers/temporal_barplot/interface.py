from analyzer_interface import WebPresenterInterface

from ..temporal import interface as temporal_interface

interface = WebPresenterInterface(
  id="time_interval_frequencies",
  version="0.1.0",
  name="Time Interval Frequencies",
  short_description="",
  base_analyzer=temporal_interface
)
