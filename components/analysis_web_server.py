import logging

from dash import Dash
from flask import Flask, render_template

from analyzer_interface import AnalyzerInterface
from analyzers import suite
from storage import Storage, Project
from terminal_tools import wait_for_key
from terminal_tools.inception import TerminalContext
from pathlib import Path

from context import WebPresenterContext
from waitress import serve
import os
import tempfile


def analysis_web_server(context: TerminalContext, storage: Storage, project: Project, analyzer: AnalyzerInterface):
  # These paths need to be resolved at runtime in order to run with
  # pyinstaller bundle
  parent_path = str(Path(__file__).resolve().parent)
  static_folder = os.path.join(parent_path, "web_static")
  template_folder = os.path.join(parent_path, "web_templates")

  web_presenters = suite.find_web_presenters(analyzer)
  web_server = Flask(
    __name__,
    template_folder=template_folder,
    static_folder=static_folder,
    static_url_path='/static'
  )
  web_server.logger.disabled = True

  temp_dirs: list[tempfile.TemporaryDirectory] = []
  for presenter in web_presenters:
    dash_app = Dash(
      presenter.server_name,
      server=web_server,
      url_base_pathname=f"/{presenter.id}/",
      external_stylesheets=['/static/dashboard_base.css']
    )
    temp_dir = tempfile.TemporaryDirectory()
    presenter_context = WebPresenterContext(
      project_id=project.id,
      primary_analyzer=analyzer,
      web_presenter=presenter,
      store=storage,
      temp_dir=temp_dir.name,
      dash_app=dash_app
    )
    temp_dirs.append(temp_dir)

    presenter.factory(presenter_context)

  @web_server.route('/')
  def index():
    return render_template(
      'index.html',
      panels=[
        (presenter.id, presenter.name)
        for presenter in web_presenters
      ],
      project_name=project.display_name,
      analyzer_name=analyzer.name,
    )

  print("Web server will run at http://localhost:8050/")
  print("Stop it with Ctrl+C")

  server_log = logging.getLogger('waitress')
  original_log_level = server_log.level
  original_disabled = server_log.disabled
  server_log.setLevel(logging.ERROR)
  server_log.disabled = True

  try:
    serve(web_server, host="127.0.0.1", port=8050)
  except Exception as ex:
    print(ex)
    wait_for_key(True)
  finally:
    server_log.setLevel(original_log_level)
    server_log.disabled = original_disabled
    print("Web server stopped")
    wait_for_key(True)
    for temp_dir in temp_dirs:
      temp_dir.cleanup()
