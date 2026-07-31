"""CLI command for starting the DOT receiver server."""

import typer
from rich.console import Console

from ..receiver.config import RECEIVER_DEFAULT_PORT, RECEIVER_DEFAULT_HOST
from ..receiver.service import run_receiver

app = typer.Typer(help="Manage DOT data receiver server")
console = Console()


@app.command("start")
def start_receiver(
    port: int = typer.Option(RECEIVER_DEFAULT_PORT, "--port", "-p", help="HTTP server port"),
    host: str = typer.Option(RECEIVER_DEFAULT_HOST, "--host", "-h", help="Bind address"),
    debug: bool = typer.Option(False, "--debug", help="Enable debug mode"),
) -> None:
    """Start the DOT data receiver server."""
    console.print("[cyan]Starting DOT receiver server...[/cyan]")
    console.print(f"[dim]Host: {host}, Port: {port}[/dim]")
    console.print(f"[dim]Endpoints available at http://{host}:{port}[/dim]")

    try:
        run_receiver(host, port, debug=debug)
    except Exception as e:
        console.print(f"[red]Error starting receiver: {e}[/red]")
        raise typer.Exit(1)
