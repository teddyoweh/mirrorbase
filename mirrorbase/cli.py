import time

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from .core import MirrorBase
from .exceptions import MirrorBaseError

console = Console()
mb = MirrorBase()


@click.group()
def cli():
    """MirrorBase: Instant live clones of Postgres databases."""
    pass


@cli.command()
@click.argument("connstring")
def connect(connstring: str):
    """Connect to a Postgres database. Available instantly, streams in background."""
    console.print("[bold]Connecting...[/bold]")
    start_time = time.time()

    def on_migration(table_name, state, done, total):
        console.print(f"  [dim]Streaming: {table_name} {state} ({done}/{total})[/dim]")

    try:
        base_id = mb.connect(connstring, on_progress=on_migration)
    except MirrorBaseError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise SystemExit(1)

    elapsed = time.time() - start_time
    status = mb.status(base_id)

    console.print(Panel.fit(
        f"[green bold]Ready![/green bold]  ({elapsed:.1f}s)\n\n"
        f"  ID:         [cyan]{base_id}[/cyan]\n"
        f"  Connection: [cyan]{status['connstring']}[/cyan]\n"
        f"  Mode:       Streaming (instant read/write, background sync)\n\n"
        f"  Clone:      [bold]mirrorbase clone {base_id}[/bold]",
        title="MirrorBase",
    ))


@cli.command()
@click.argument("base_id")
@click.option("--name", default=None, help="Custom clone name")
def clone(base_id: str, name: str | None):
    """Create an instant CoW clone."""
    console.print("[bold]Creating clone...[/bold]")
    start_time = time.time()

    try:
        clone_id, connstring = mb.clone(base_id, clone_id=name)
    except MirrorBaseError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise SystemExit(1)

    elapsed = time.time() - start_time

    console.print(Panel.fit(
        f"[green bold]Clone ready![/green bold]  ({elapsed:.1f}s)\n\n"
        f"  ID:         [cyan]{clone_id}[/cyan]\n"
        f"  Connection: [cyan]{connstring}[/cyan]\n\n"
        f"  Connect:    psql '{connstring}'\n"
        f"  Destroy:    mirrorbase destroy {clone_id}",
        title="MirrorBase Clone",
    ))


@cli.command()
@click.argument("base_id", required=False)
def status(base_id: str | None):
    """Show status of base replicas."""
    if base_id:
        try:
            info = mb.status(base_id)
        except MirrorBaseError as e:
            console.print(f"[red]Error:[/red] {e}")
            raise SystemExit(1)

        table = Table(title=f"Base: {base_id}")
        table.add_column("Property")
        table.add_column("Value")
        table.add_row("State", info["state"])
        table.add_row("Running", str(info["running"]))
        table.add_row("Port", str(info["port"]))
        table.add_row("Connection", info["connstring"])
        table.add_row("Sync Mode", info["sync_mode"])
        mig = info.get("migration", {})
        if mig.get("status") == "streaming":
            table.add_row("Migration", f"{mig['migrated']}/{mig['total']} tables local")
        else:
            table.add_row("Migration", "Complete (fully local)")
        console.print(table)
    else:
        bases = mb.list_bases()
        if not bases:
            console.print("[dim]No base replicas found.[/dim]")
            return
        table = Table(title="Base Replicas")
        table.add_column("ID")
        table.add_column("State")
        table.add_column("Port")
        table.add_column("Source DB")
        table.add_column("Mode")
        table.add_column("Created")
        for b in bases:
            table.add_row(
                b["base_id"], b["state"], str(b["port"]),
                b["source_dbname"], b.get("sync_mode", ""), b["created_at"],
            )
        console.print(table)


@cli.command(name="list")
@click.option("--base", default=None, help="Filter by base ID")
def list_clones(base: str | None):
    """List all clones."""
    clones = mb.list_clones(base_id=base)
    if not clones:
        console.print("[dim]No clones found.[/dim]")
        return
    table = Table(title="Clones")
    table.add_column("Clone ID")
    table.add_column("Base ID")
    table.add_column("Port")
    table.add_column("DB")
    table.add_column("State")
    table.add_column("Created")
    for c in clones:
        table.add_row(
            c["clone_id"], c["base_id"], str(c["port"]),
            c.get("source_dbname", ""), c["state"], c["created_at"],
        )
    console.print(table)


@cli.command()
@click.argument("clone_id")
def destroy(clone_id: str):
    """Destroy a clone."""
    try:
        mb.destroy(clone_id)
    except MirrorBaseError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise SystemExit(1)
    console.print(f"[green]Clone {clone_id} destroyed.[/green]")


@cli.command()
@click.argument("base_id")
@click.confirmation_option(prompt="This will destroy the base and all clones. Continue?")
def teardown(base_id: str):
    """Tear down a base replica and all its clones."""
    try:
        mb.teardown(base_id)
    except MirrorBaseError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise SystemExit(1)
    console.print(f"[green]Base {base_id} and all clones torn down.[/green]")


@cli.command()
@click.option("--host", default="0.0.0.0", help="Bind address")
@click.option("--port", default=8100, help="API port")
def serve(host: str, port: int):
    """Start the MirrorBase REST API server."""
    from .server import serve as run_server
    run_server(host=host, port=port)


if __name__ == "__main__":
    cli()
