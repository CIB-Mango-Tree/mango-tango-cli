import sys
import time
from multiprocessing import Process, Value, Event, Manager

_spinner_frames = [
  '▁', '▁', '▂', '▂', '▃', '▃', '▂', '▂', '▁',  # bouncy bouncy
  '▁', '▂', '▃', '▄', '▅', '▆', '▇', '█', '▇', '▆', '▅', '▄', '▃', '▂'
]


class ProgressReporter:
  def __init__(self, title: str):
    self.title = title
    self.progress = Value('d', -1)
    self.done_text = Manager().dict()
    self.process = Process(target=self._run)
    self.done_event = Event()
    self.spinner_frame_index = 0
    self.last_output_length = 0

  def start(self):
    self.process.start()

  def update(self, value: float):
    with self.progress.get_lock():
      self.progress.value = max(min(value, 1), 0)

  def finish(self, done_text: str = "Done!"):
    self.done_text["done"] = done_text
    self.done_event.set()
    self.process.join()

  def __enter__(self):
    self.start()
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self.finish()

  def _run(self):
    try:
      while not self.done_event.is_set():
        with self.progress.get_lock():
          current_progress = self.progress.value
        self.spinner_frame_index = (
          (self.spinner_frame_index + 1) % len(_spinner_frames)
        )
        progress_text = (
          f"{current_progress * 100:.2f}%"
          if current_progress >= 0 else "..."
        )
        self._draw(progress_text)
        time.sleep(0.1)
      self._draw(self.done_text.get("done", "Done!"), "✅")
    except KeyboardInterrupt:
      pass
    finally:
      sys.stdout.write("\n")
      sys.stdout.flush()

  def _draw(self, text: str, override_spinner_frame: str = None):
    output = (
      f"{override_spinner_frame or _spinner_frames[self.spinner_frame_index]} "
      f"{self.title} {text}"
    )
    output_with_spaces = output.ljust(self.last_output_length)
    sys.stdout.write("\r" + output_with_spaces)
    sys.stdout.flush()
    self.last_output_length = len(output)
