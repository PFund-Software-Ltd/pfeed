"""`pfeed data <path>`: opens a data explorer on a parquet/duckdb/ducklake/sqlite path, read-only."""
from __future__ import annotations

from typing import TYPE_CHECKING, Literal, cast

if TYPE_CHECKING:
    import duckdb

import contextlib
import json
import signal
import sqlite3
import time
import uuid
import webbrowser
from pathlib import Path

import click

type DataKind = Literal['parquet', 'duckdb', 'ducklake', 'sqlite']


def _is_ducklake_catalog(path: Path) -> bool:
    with contextlib.closing(sqlite3.connect(f'{path.as_uri()}?mode=ro', uri=True)) as conn:
        query = "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'ducklake_metadata'"
        return conn.execute(query).fetchone() is not None


def _detect_file(path: Path) -> DataKind | None:
    """Detects by magic bytes, since e.g. '.db' can be sqlite, duckdb or a ducklake catalog."""
    with path.open('rb') as f:
        header = f.read(16)
    if header.startswith(b'PAR1'):
        return 'parquet'
    if header[8:12] == b'DUCK':
        return 'duckdb'
    if header.startswith(b'SQLite format 3\x00'):
        return 'ducklake' if _is_ducklake_catalog(path) else 'sqlite'
    return None


def _detect(path: Path) -> tuple[DataKind, Path]:
    """Returns the data kind and the path to open, i.e. the catalog file when PATH is a ducklake folder."""
    if path.is_file():
        if kind := _detect_file(path):
            return kind, path
        raise click.ClickException(f'{path} is not a parquet, duckdb, ducklake or sqlite file')
    # a ducklake folder also holds parquet files, so its catalog is looked for first
    for child in sorted(path.iterdir()):
        if child.is_file() and _detect_file(child) == 'ducklake':
            return 'ducklake', child
    if next(path.rglob('*.parquet'), None):
        return 'parquet', path
    raise click.ClickException(f'No parquet files or ducklake catalog found in {path}')


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _connect(kind: DataKind, path: Path, name: str) -> duckdb.DuckDBPyConnection:
    """Opens an in-memory session with PATH attached read-only as `name`, a view for parquet."""
    import duckdb

    conn = duckdb.connect()
    conn.execute("SET TimeZone = 'UTC'")
    name = _quote_identifier(name)
    if kind == 'parquet':
        source = f'{path}/**/*.parquet' if path.is_dir() else str(path)
        conn.execute(f'CREATE VIEW {name} AS SELECT * FROM read_parquet({_quote_literal(source)})')
    elif kind == 'ducklake':
        conn.execute(f'ATTACH {_quote_literal(f"ducklake:sqlite:{path}")} AS {name} (READ_ONLY)')
    elif kind == 'duckdb':
        conn.execute(f'ATTACH {_quote_literal(str(path))} AS {name} (READ_ONLY)')
    elif kind == 'sqlite':
        conn.execute(f'ATTACH {_quote_literal(str(path))} AS {name} (TYPE sqlite, READ_ONLY)')
    return conn


_NOTEBOOK_STORE_DIR_NAME = '.duckdb_ui'
# same schema the DuckDB ui extension creates in its own notebook store
_NOTEBOOK_STORE_SCHEMA = '''
CREATE TABLE IF NOT EXISTS _duckdb_ui.current_notebook_id(id UUID NOT NULL);
CREATE TABLE IF NOT EXISTS _duckdb_ui.has_onboarded(has_onboarded BOOLEAN);
CREATE TABLE IF NOT EXISTS _duckdb_ui.notebooks(id UUID PRIMARY KEY, "name" VARCHAR NOT NULL, created TIMESTAMP NOT NULL);
CREATE TABLE IF NOT EXISTS _duckdb_ui.notebook_versions(
    notebook_id UUID, "version" INTEGER, title VARCHAR NOT NULL, "json" VARCHAR NOT NULL,
    created TIMESTAMP NOT NULL, expires TIMESTAMP, PRIMARY KEY(notebook_id, "version")
);
'''


def _attach_notebook_store(conn: duckdb.DuckDBPyConnection, data_path: Path) -> None:
    """Attaches pfeed's own notebook store as `_duckdb_ui`, which the ui extension then uses
    instead of the user's ~/.duckdb one, so pfeed never touches (or gets locked out by) the user's notebooks.
    """
    import duckdb

    store_dir = data_path / _NOTEBOOK_STORE_DIR_NAME
    store_dir.mkdir(parents=True, exist_ok=True)
    try:
        conn.execute(f'ATTACH {_quote_literal(str(store_dir / "ui.db"))} AS _duckdb_ui')
    except duckdb.IOException as e:
        if 'lock' not in str(e).lower():
            raise
        raise click.ClickException(
            f'Another `pfeed data` is already running on the data path {data_path}, close it first'
        ) from None
    conn.execute(_NOTEBOOK_STORE_SCHEMA)


# fixed, so a scratch notebook left behind by a killed session is replaced on the next launch, not piled up
_SCRATCH_NOTEBOOK_ID = uuid.UUID('00000000-0000-0000-0000-00000000feed')


def _write_notebook(conn: duckdb.DuckDBPyConnection, kind: DataKind, name: str, title: str, keep: bool) -> uuid.UUID:
    """Writes the notebook the ui opens with, one cell to start exploring the data from.

    Args:
        name: name of the attached database, or of the view for parquet.
        title: shown in the ui's notebook list.
        keep: if True, the notebook gets its own id, so it is never replaced or deleted by pfeed.
    """
    if kind == 'parquet':
        query, database = f'SELECT * FROM {_quote_identifier(name)} LIMIT 100', 'memory'
    else:
        query, database = f'SELECT schema, name FROM (SHOW ALL TABLES) WHERE database = {_quote_literal(name)}', name
    notebook = {
        'cells': [{'query': query, 'cellId': 1, 'useDatabase': database}],
        'currentDatabase': None,
        'viewMode': {'mode': 'default'},
        'version': 1,
        'notebookSerializationFormat': 3,
    }
    notebook_id = uuid.uuid4() if keep else _SCRATCH_NOTEBOOK_ID
    _delete_notebook(conn, notebook_id)
    conn.execute("INSERT INTO _duckdb_ui.notebooks VALUES (?, ?, now())", [notebook_id, f'pfeed_{notebook_id}'])
    conn.execute(
        "INSERT INTO _duckdb_ui.notebook_versions VALUES (?, 1, ?, ?, now(), NULL)",
        [notebook_id, title, json.dumps(notebook)],
    )
    conn.execute('DELETE FROM _duckdb_ui.current_notebook_id')
    conn.execute('INSERT INTO _duckdb_ui.current_notebook_id VALUES (?)', [notebook_id])
    # skips the ui's onboarding screen
    conn.execute('DELETE FROM _duckdb_ui.has_onboarded')
    conn.execute('INSERT INTO _duckdb_ui.has_onboarded VALUES (true)')
    return notebook_id


def _delete_notebook(conn: duckdb.DuckDBPyConnection, notebook_id: uuid.UUID) -> None:
    conn.execute('DELETE FROM _duckdb_ui.notebook_versions WHERE notebook_id = ?', [notebook_id])
    conn.execute('DELETE FROM _duckdb_ui.notebooks WHERE id = ?', [notebook_id])
    conn.execute('DELETE FROM _duckdb_ui.current_notebook_id WHERE id = ?', [notebook_id])


@click.command()
@click.argument('path', type=click.Path(path_type=Path))
@click.option('--keep', is_flag=True, help='Keep the notebook after exit (default: deleted on exit)')
@click.option('--port', type=int, default=None, help='Port of the data explorer (default: a free port)')
@click.option(
    '--data-path',
    type=click.Path(path_type=Path, file_okay=False),
    default=None,
    help="Override the data path, which relative PATHs resolve against and the notebooks are stored in (default: pfeed's data path)",
)
@click.pass_context
def data(ctx: click.Context, path: Path, keep: bool, port: int | None, data_path: Path | None) -> None:
    """Explore data under PATH, relative paths resolve against the data path."""
    data_path = (data_path or ctx.obj['config'].data_path).expanduser()
    title = str(path)
    path = path.expanduser()
    if not path.is_absolute():
        path = data_path / path
    if not path.exists():
        raise click.ClickException(f'{path} does not exist')
    kind, path = _detect(path)
    name = path.stem
    conn = _connect(kind, path, name)
    _attach_notebook_store(conn, data_path)
    notebook_id = _write_notebook(conn, kind, name, title, keep)
    if port is None:
        from pfund_kit.utils import get_free_port

        # not DuckDB's default port, so e.g. pfeed and alphafund (different data paths) can run side by side
        port = get_free_port()
    conn.execute(f'SET ui_local_port = {port}')
    try:
        # returns a message instead of raising, e.g. when another DuckDB UI already serves the port
        message = cast('tuple[str]', conn.execute('CALL start_ui_server()').fetchone())[0]
        if not message.startswith('UI server started'):
            raise click.ClickException(f'{message}, try another --port')
        # opens the notebook by its url, the ui's own "last used notebook" is not reliable across sessions
        url = f'http://localhost:{port}/notebooks/pfeed-{notebook_id}'
        webbrowser.open(url)
        click.echo(f'Data explorer at {url}  (Ctrl-C to stop)')
        if not keep:
            click.echo('The notebook is temporary and deleted on exit, run with --keep to keep it')
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        # a second Ctrl-C (e.g. forwarded by a wrapper like pixi, or pressed twice) would abort the cleanup
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        conn.execute('CALL stop_ui_server()')
        if not keep:
            _delete_notebook(conn, notebook_id)
        conn.close()
