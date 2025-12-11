# backend/cli.py
import argparse
import asyncio
from importlib.resources import files

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .app import create_app


def create_frontend_app() -> FastAPI:
    """
    Build a small FastAPI app that serves the built frontend
    from backend/frontend on a separate port.
    """
    app = FastAPI(title="CVAT Dataset Converter UI")

    # Locate the 'frontend' directory inside the installed package
    pkg_root = files(__package__)
    frontend_dir = pkg_root / "frontend"

    if not frontend_dir.is_dir():
        raise RuntimeError(f"Frontend directory not found at {frontend_dir!s}")

    # Serve index.html and all assets from "/"
    # - GET /         -> index.html
    # - GET /css/...  -> CSS
    # - GET /js/...   -> JS
    app.mount(
        "/",
        StaticFiles(directory=str(frontend_dir), html=True),
        name="frontend",
    )

    return app


def main() -> None:
    """
    Entry point for the `cvat_dataset_convert` CLI.
    Starts:
      - backend FastAPI API (default port 6007)
      - frontend static UI (default port 6006)
    """
    parser = argparse.ArgumentParser(
        prog="cvat_dataset_convert",
        description="Run CVAT Dataset Converter UI and backend.",
    )
    parser.add_argument(
        "--host",
        "-H",
        default="127.0.0.1",
        help="Host/IP to bind (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--api-port",
        "-a",
        type=int,
        default=6007,
        help="Port for the FastAPI backend (default: 6007)",
    )
    parser.add_argument(
        "--ui-port",
        "-u",
        type=int,
        default=6006,
        help="Port for the web UI (default: 6006)",
    )
    parser.add_argument(
        "--no-prompt",
        action="store_true",
        help="Do not ask interactive questions; just use the CLI arguments.",
    )

    args = parser.parse_args()

    host = args.host
    api_port = args.api_port
    ui_port = args.ui_port

    if not args.no_prompt:
        print("CVAT Dataset Converter")
        print("----------------------")
        print("Press Enter to accept the value in [brackets].\n")

        host_in = input(f"Host [{host}]: ").strip()
        if host_in:
            host = host_in

        api_in = input(f"Backend (API) port [{api_port}]: ").strip()
        if api_in:
            api_port = int(api_in)

        ui_in = input(f"Frontend (UI) port [{ui_port}]: ").strip()
        if ui_in:
            ui_port = int(ui_in)

    # Build the two FastAPI apps
    api_app = create_app()
    frontend_app = create_frontend_app()

    # Configure two uvicorn servers
    config_api = uvicorn.Config(api_app, host=host, port=api_port, log_level="info")
    config_ui = uvicorn.Config(frontend_app, host=host, port=ui_port, log_level="info")

    server_api = uvicorn.Server(config_api)
    server_ui = uvicorn.Server(config_ui)

    async def run_both():
        print(f"Backend (API) running on http://{host}:{api_port}")
        print(f"Frontend (UI) running on http://{host}:{ui_port}")
        print("Press Ctrl+C to stop.")
        await asyncio.gather(
            server_api.serve(),
            server_ui.serve(),
        )

    asyncio.run(run_both())
