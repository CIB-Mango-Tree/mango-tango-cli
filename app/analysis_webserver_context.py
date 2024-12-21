import logging
import os.path
from pathlib import Path
from tempfile import TemporaryDirectory

from dash import Dash
from flask import Flask, render_template
from pydantic import BaseModel
from waitress import serve

from context import WebPresenterContext

from .analysis_context import AnalysisContext
from .app_context import AppContext


class AnalysisWebServerContext(BaseModel):
    app_context: AppContext
    analysis_context: AnalysisContext

    def start(self):
        containing_dir = str(Path(__file__).resolve().parent)
        static_folder = os.path.join(containing_dir, "web_static")
        template_folder = os.path.join(containing_dir, "web_templates")

        web_presenters = self.analysis_context.web_presenters
        web_server = Flask(
            __name__,
            template_folder=template_folder,
            static_folder=static_folder,
            static_url_path="/static",
        )
        web_server.logger.disabled = True
        temp_dirs: list[TemporaryDirectory] = []

        for presenter in web_presenters:
            dash_app = Dash(
                presenter.server_name,
                server=web_server,
                url_base_pathname=f"/{presenter.id}/",
                external_stylesheets=["/static/dashboard_base.css"],
            )
            temp_dir = TemporaryDirectory()
            presenter_context = WebPresenterContext(
                analysis=self.analysis_context.model,
                web_presenter=presenter,
                store=self.app_context.storage,
                temp_dir=temp_dir.name,
                dash_app=dash_app,
            )
            temp_dirs.append(temp_dir)
            presenter.factory(presenter_context)

        project_name = self.analysis_context.project_context.display_name
        analyzer_name = self.analysis_context.display_name

        @web_server.route("/")
        def index():
            return render_template(
                "index.html",
                panels=[(presenter.id, presenter.name) for presenter in web_presenters],
                project_name=project_name,
                analyzer_name=analyzer_name,
            )

        server_log = logging.getLogger("waitress")
        original_log_level = server_log.level
        original_disabled = server_log.disabled
        server_log.setLevel(logging.ERROR)
        server_log.disabled = True

        try:
            serve(web_server, host="127.0.0.1", port=8050)
        finally:
            server_log.setLevel(original_log_level)
            server_log.disabled = original_disabled
            for temp_dir in temp_dirs:
                temp_dir.cleanup()
