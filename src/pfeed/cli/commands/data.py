# VIBE-CODED, will be removed when theres a UI for pfeed.
from __future__ import annotations

# ruff: noqa

import datetime
import json
import re
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

import click

BackendFormat = Literal["parquet", "deltalake", "sqlite", "duckdb", "lancedb"]

PFEED_PARQUET_FILE_RE = re.compile(
    r"^(?P<symbol>.+)_(?P<date>\d{4}-\d{2}-\d{2})\.parquet$"
)
HIVE_PART_RE = re.compile(r"^([^=]+)=(.*)$")
DUCKDB_LOCK_RE = re.compile(
    r"Conflicting lock is held in (\S+).*?\(PID (\d+)\)", re.DOTALL
)

HIVE_ORDER = [
    "env",
    "data_layer",
    "data_domain",
    "data_source",
    "data_origin",
    "asset_type",
    "resolution",
]

# Columns that may auto-hide from `ls` when every visible row carries the same value.
COLLAPSIBLE_COLUMNS = (
    "storage",
    "env",
    "data_layer",
    "data_domain",
    "data_source",
    "asset_type",
    "resolution",
    "backend",
)
COLUMN_LABELS = {
    "storage": "Storage",
    "env": "Env",
    "data_layer": "Layer",
    "data_domain": "Domain",
    "data_source": "Source",
    "asset_type": "AssetType",
    "resolution": "Resolution",
    "backend": "Backend",
}


@dataclass
class DataUnit:
    storage: str
    backend: BackendFormat
    env: str = ""
    data_layer: str = ""
    data_domain: str = ""
    data_source: str = ""
    data_origin: str = ""
    asset_type: str = ""
    resolution: str = ""
    symbols: list[str] = field(default_factory=list)
    dates: list[datetime.date] = field(default_factory=list)
    size_bytes: int = 0
    last_modified: datetime.datetime | None = None
    path: str = ""
    status: str = ""  # "" (readable), "locked", "unreadable"

    @property
    def is_readable(self) -> bool:
        return self.status == ""

    @property
    def latest_date(self) -> datetime.date | None:
        return max(self.dates) if self.dates else None

    @property
    def earliest_date(self) -> datetime.date | None:
        return min(self.dates) if self.dates else None

    @property
    def date_range(self) -> str:
        if not self.dates:
            return "—"
        lo, hi = self.earliest_date, self.latest_date
        return f"{lo} → {hi}" if lo != hi else str(lo)

    @property
    def symbol_cell(self) -> str:
        if self.status == "locked":
            return "[yellow]<locked>[/yellow]"
        if self.status == "unreadable":
            return "[red]<unreadable>[/red]"
        if not self.symbols:
            return "—"
        return (
            self.symbols[0]
            if len(self.symbols) == 1
            else f"<{len(self.symbols)} products>"
        )

    @property
    def source_cell(self) -> str:
        if self.data_origin and self.data_origin != self.data_source:
            return f"{self.data_source}:{self.data_origin}"
        return self.data_source or "—"


def _fmt_size(n: int) -> str:
    if n <= 0:
        return "—"
    units = ["B", "KB", "MB", "GB", "TB"]
    f = float(n)
    i = 0
    while f >= 1024 and i < len(units) - 1:
        f /= 1024
        i += 1
    return f"{f:.1f} {units[i]}"


def _fmt_relative_time(dt: datetime.datetime | None) -> str:
    if dt is None:
        return "—"
    now = datetime.datetime.now(dt.tzinfo) if dt.tzinfo else datetime.datetime.now()
    delta = now - dt
    secs = int(delta.total_seconds())
    if secs < 60:
        return f"{secs}s ago"
    if secs < 3600:
        return f"{secs // 60}m ago"
    if secs < 86400:
        return f"{secs // 3600}h ago"
    days = secs // 86400
    if days < 30:
        return f"{days}d ago"
    if days < 365:
        return f"{days // 30}mo ago"
    return f"{days // 365}y ago"


# ---------- Discovery: file-based hive layout ----------


def _walk_hive(root: Path) -> Iterator[tuple[dict[str, str], Path]]:
    if not root.exists():
        return

    def rec(
        d: Path, depth: int, ctx: dict[str, str]
    ) -> Iterator[tuple[dict[str, str], Path]]:
        if depth == len(HIVE_ORDER):
            yield ctx, d
            return
        key = HIVE_ORDER[depth]
        try:
            children = sorted(d.iterdir())
        except OSError:
            return
        for c in children:
            if not c.is_dir():
                continue
            m = HIVE_PART_RE.match(c.name)
            if not m or m.group(1) != key:
                continue
            yield from rec(c, depth + 1, {**ctx, key: m.group(2)})

    yield from rec(root, 0, {})


def _read_delta_metadata(table_dir: Path) -> dict[str, Any] | None:
    """Delta's sidecar metadata is embedded in the parquet *schema*-level
    `metadata_json` blob (same as per-file parquet metadata), not in rows.
    """
    meta_file = table_dir / "deltalake_metadata.parquet"
    if not meta_file.is_file():
        return None
    try:
        import pyarrow.parquet as pq

        schema_md = pq.read_schema(str(meta_file)).metadata or {}
        raw = schema_md.get(b"metadata_json")
        if raw:
            return json.loads(raw)
    except Exception:
        pass
    return None


def _parse_dates_field(field_val: Any) -> list[datetime.date]:
    out: list[datetime.date] = []
    if not field_val:
        return out
    for ds in field_val:
        try:
            if isinstance(ds, datetime.date):
                out.append(ds)
            elif isinstance(ds, str):
                out.append(datetime.date.fromisoformat(ds))
        except Exception:
            pass
    return out


def _scan_delta(
    table_dir: Path,
) -> tuple[list[datetime.date], list[str], int, datetime.datetime | None]:
    dates: set[datetime.date] = set()
    symbols: list[str] = []
    size = 0
    latest_mtime: float = 0.0

    md = _read_delta_metadata(table_dir)
    if md:
        dates.update(_parse_dates_field(md.get("dates")))
        products = md.get("products") or {}
        if isinstance(products, dict):
            symbols = sorted(products.keys())

    if not dates:
        for daydir in table_dir.rglob("day=*"):
            try:
                mday = HIVE_PART_RE.match(daydir.name)
                mmon = HIVE_PART_RE.match(daydir.parent.name)
                myr = HIVE_PART_RE.match(daydir.parent.parent.name)
                if mday and mmon and myr:
                    dates.add(
                        datetime.date(
                            int(myr.group(2)), int(mmon.group(2)), int(mday.group(2))
                        )
                    )
            except Exception:
                pass

    for f in table_dir.rglob("*.parquet"):
        if f.name == "deltalake_metadata.parquet":
            continue
        try:
            st = f.stat()
            size += st.st_size
            latest_mtime = max(latest_mtime, st.st_mtime)
        except OSError:
            pass

    mtime = datetime.datetime.fromtimestamp(latest_mtime) if latest_mtime else None
    return sorted(dates), symbols, size, mtime


def _discover_file_based(storage_name: str, data_path: Path) -> Iterator[DataUnit]:
    for ctx, leaf in _walk_hive(data_path):
        has_delta = (leaf / "_delta_log").is_dir()
        if has_delta:
            dates, symbols, size, mtime = _scan_delta(leaf)
            yield DataUnit(
                storage=storage_name,
                backend="deltalake",
                env=ctx.get("env", ""),
                data_layer=ctx.get("data_layer", ""),
                data_domain=ctx.get("data_domain", ""),
                data_source=ctx.get("data_source", ""),
                data_origin=ctx.get("data_origin", ""),
                asset_type=ctx.get("asset_type", ""),
                resolution=ctx.get("resolution", ""),
                symbols=symbols,
                dates=dates,
                size_bytes=size,
                last_modified=mtime,
                path=str(leaf),
            )

        by_symbol: dict[str, dict[str, Any]] = {}
        for f in leaf.rglob("*.parquet"):
            m = PFEED_PARQUET_FILE_RE.match(f.name)
            if not m:
                continue
            sym = m.group("symbol")
            try:
                d = datetime.date.fromisoformat(m.group("date"))
            except ValueError:
                continue
            e = by_symbol.setdefault(sym, {"dates": [], "size": 0, "mtime": 0.0})
            e["dates"].append(d)
            try:
                st = f.stat()
                e["size"] += st.st_size
                e["mtime"] = max(e["mtime"], st.st_mtime)
            except OSError:
                pass

        for sym, info in sorted(by_symbol.items()):
            mtime = (
                datetime.datetime.fromtimestamp(info["mtime"])
                if info["mtime"]
                else None
            )
            yield DataUnit(
                storage=storage_name,
                backend="parquet",
                env=ctx.get("env", ""),
                data_layer=ctx.get("data_layer", ""),
                data_domain=ctx.get("data_domain", ""),
                data_source=ctx.get("data_source", ""),
                data_origin=ctx.get("data_origin", ""),
                asset_type=ctx.get("asset_type", ""),
                resolution=ctx.get("resolution", ""),
                symbols=[sym],
                dates=sorted(info["dates"]),
                size_bytes=info["size"],
                last_modified=mtime,
                path=str(leaf),
            )


# ---------- Discovery: DuckDB ----------


def _split_duckdb_schema_name(schema_name: str) -> tuple[str, str, str, str] | None:
    """schema = '{layer}_{domain}_{source}_{origin}' lower. Layer is the first known token."""
    try:
        from pfeed.enums import DataLayer
    except Exception:
        return None
    layer_names = {l.name.lower() for l in DataLayer}
    parts = schema_name.split("_")
    if len(parts) < 4 or parts[0] not in layer_names:
        return None
    layer = parts[0].upper()
    source = parts[-2].upper()
    origin = parts[-1].upper()
    domain = "_".join(parts[1:-2]).upper() or "MARKET_DATA"
    return layer, domain, source, origin


def _extract_hive_context_from_path(file: Path, data_root: Path) -> dict[str, str]:
    """Recover hive-partition fields from the directories above a file.

    DuckDB files placed inside a hive layout (e.g.
    `env=BACKTEST/data_layer=CLEANED/.../resolution=1_MINUTE/backtest.duckdb`)
    let us recover env/layer/source/etc. without opening the file.
    """
    ctx: dict[str, str] = {}
    try:
        rel = file.parent.relative_to(data_root)
    except ValueError:
        return ctx
    for part in rel.parts:
        m = HIVE_PART_RE.match(part)
        if m:
            ctx[m.group(1)] = m.group(2)
    return ctx


def _discover_duckdb(
    roots: list[tuple[str, Path]], errors: list[str] | None = None
) -> Iterator[DataUnit]:
    """Walk every configured root for *.duckdb files.

    Storage is the file-based parent (LOCAL/CACHE — where the file lives);
    Backend is the format (`duckdb`). Same Storage/Backend semantics as
    parquet/delta rows so dimensions are consistent across all storage types.
    """
    try:
        import duckdb
    except ImportError:
        return

    seen: set[Path] = set()
    for storage_label, root in roots:
        if not root.exists():
            continue
        for db_file in sorted(root.rglob("*.duckdb")):
            try:
                resolved = db_file.resolve()
            except OSError:
                resolved = db_file
            if resolved in seen:
                continue
            seen.add(resolved)
            yield from _process_duckdb_file(db_file, root, storage_label, errors)


def _process_duckdb_file(
    db_file: Path, root: Path, storage_label: str, errors: list[str] | None
) -> Iterator[DataUnit]:
    """Emit DataUnit(s) for one *.duckdb file. Storage=storage_label, Backend=duckdb."""
    import duckdb

    env_from_stem = db_file.stem.upper()
    try:
        db_mtime = datetime.datetime.fromtimestamp(db_file.stat().st_mtime)
        db_size = db_file.stat().st_size
    except OSError:
        db_mtime = None
        db_size = 0

    try:
        con = duckdb.connect(str(db_file), read_only=True)
    except Exception as exc:
        msg = str(exc)
        lock_match = DUCKDB_LOCK_RE.search(msg)
        is_lock = lock_match is not None
        hive_ctx = _extract_hive_context_from_path(db_file, root)
        if errors is not None:
            if is_lock:
                program = lock_match.group(1).rsplit("/", 1)[-1]
                pid = lock_match.group(2)
                errors.append(
                    f"DuckDB locked: {db_file}\n"
                    f"  Held by {program} (PID {pid}). Close that process and re-run "
                    f"`pfeed data` to read its contents."
                )
            else:
                short = msg.splitlines()[0][:160]
                errors.append(f"DuckDB unreadable: {db_file} — {short}")
        yield DataUnit(
            storage=storage_label,
            backend="duckdb",
            env=hive_ctx.get("env", env_from_stem),
            data_layer=hive_ctx.get("data_layer", ""),
            data_domain=hive_ctx.get("data_domain", ""),
            data_source=hive_ctx.get("data_source", ""),
            data_origin=hive_ctx.get("data_origin", ""),
            asset_type=hive_ctx.get("asset_type", ""),
            resolution=hive_ctx.get("resolution", ""),
            symbols=[],
            dates=[],
            size_bytes=db_size,
            last_modified=db_mtime,
            path=str(db_file),
            status="locked" if is_lock else "unreadable",
        )
        return

    env = env_from_stem
    try:
        schemas = con.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'main')"
        ).fetchall()
        schema_count = max(sum(1 for _ in schemas), 1)

        for (schema_name,) in schemas:
            parsed = _split_duckdb_schema_name(schema_name)
            if not parsed:
                continue
            layer, domain, source, origin = parsed

            tables = con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
                [schema_name],
            ).fetchall()
            non_meta = [t for (t,) in tables if t != "metadata"]
            table_count = max(len(non_meta), 1)

            for table_name in non_meta:
                md: dict[str, Any] = {}
                try:
                    row = con.execute(
                        f'SELECT metadata_json FROM "{schema_name}"."metadata" WHERE table_name = ?',
                        [table_name],
                    ).fetchone()
                    if row and row[0]:
                        md = json.loads(row[0])
                except Exception:
                    pass

                asset_type = (md.get("asset_type") or "").upper()
                resolution = md.get("resolution") or ""
                products = md.get("products") or {}
                symbols = sorted(products.keys()) if isinstance(products, dict) else []
                dates = sorted(set(_parse_dates_field(md.get("dates"))))

                if not asset_type or not resolution:
                    toks = table_name.split("_")
                    if toks:
                        asset_type = asset_type or toks[0].upper()
                        resolution = resolution or "_".join(toks[1:]).upper()

                # File size is per-db file, attribute proportionally across tables for display.
                yield DataUnit(
                    storage=storage_label,
                    backend="duckdb",
                    env=env,
                    data_layer=layer,
                    data_domain=domain,
                    data_source=source,
                    data_origin=origin,
                    asset_type=asset_type,
                    resolution=resolution,
                    symbols=symbols,
                    dates=dates,
                    size_bytes=db_size // (schema_count * table_count),
                    last_modified=db_mtime,
                    path=f"{db_file}::{schema_name}.{table_name}",
                )
    finally:
        try:
            con.close()
        except Exception:
            pass


# ---------- Discovery: SQLite ----------


def _discover_sqlite(
    roots: list[tuple[str, Path]], errors: list[str] | None = None
) -> Iterator[DataUnit]:
    """Walk configured roots for pfeed-managed *.sqlite files."""
    seen: set[Path] = set()
    for storage_label, root in roots:
        if not root.exists():
            continue
        for db_file in sorted(root.rglob("*.sqlite")):
            try:
                resolved = db_file.resolve()
            except OSError:
                resolved = db_file
            if resolved in seen:
                continue
            seen.add(resolved)
            yield from _process_sqlite_file(db_file, root, storage_label, errors)


def _process_sqlite_file(
    db_file: Path, root: Path, storage_label: str, errors: list[str] | None
) -> Iterator[DataUnit]:
    """Emit DataUnit(s) from SQLiteIO's quoted `schema.table` layout."""
    import sqlite3

    env_from_stem = db_file.stem.upper()
    try:
        db_mtime = datetime.datetime.fromtimestamp(db_file.stat().st_mtime)
        db_size = db_file.stat().st_size
    except OSError:
        db_mtime = None
        db_size = 0

    con = None
    try:
        uri = db_file.resolve().as_uri() + "?mode=ro"
        con = sqlite3.connect(uri, uri=True, timeout=0.1)
        physical_tables = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        metadata_suffix = ".metadata"
        metadata_tables = sorted(
            name for name in physical_tables if name.endswith(metadata_suffix)
        )
        units: list[DataUnit] = []
        schema_count = max(len(metadata_tables), 1)

        for metadata_table in metadata_tables:
            schema_name = metadata_table[: -len(metadata_suffix)]
            parsed = _split_duckdb_schema_name(schema_name)
            if not parsed:
                continue
            layer, domain, source, origin = parsed
            quoted_metadata = '"' + metadata_table.replace('"', '""') + '"'
            rows = con.execute(
                f"SELECT table_name, metadata_json FROM {quoted_metadata}"
            ).fetchall()
            table_count = max(len(rows), 1)

            for table_name, metadata_json in rows:
                if f"{schema_name}.{table_name}" not in physical_tables:
                    continue
                md: dict[str, Any] = {}
                if metadata_json:
                    try:
                        md = json.loads(metadata_json)
                    except (TypeError, json.JSONDecodeError):
                        pass

                asset_type = (md.get("asset_type") or "").upper()
                resolution = md.get("resolution") or ""
                products = md.get("products") or {}
                symbols = sorted(products.keys()) if isinstance(products, dict) else []
                dates = sorted(set(_parse_dates_field(md.get("dates"))))
                if not asset_type or not resolution:
                    tokens = table_name.split("_")
                    if tokens:
                        asset_type = asset_type or tokens[0].upper()
                        resolution = resolution or "_".join(tokens[1:]).upper()

                units.append(
                    DataUnit(
                        storage=storage_label,
                        backend="sqlite",
                        env=env_from_stem,
                        data_layer=layer,
                        data_domain=domain,
                        data_source=source,
                        data_origin=origin,
                        asset_type=asset_type,
                        resolution=resolution,
                        symbols=symbols,
                        dates=dates,
                        size_bytes=db_size // (schema_count * table_count),
                        last_modified=db_mtime,
                        path=f"{db_file}::{schema_name}.{table_name}",
                    )
                )
        yield from units
    except Exception as exc:
        message = str(exc)
        is_locked = "locked" in message.lower()
        if errors is not None:
            status = "locked" if is_locked else "unreadable"
            errors.append(
                f"SQLite {status}: {db_file} — {message.splitlines()[0][:160]}"
            )
        hive_ctx = _extract_hive_context_from_path(db_file, root)
        yield DataUnit(
            storage=storage_label,
            backend="sqlite",
            env=hive_ctx.get("env", env_from_stem),
            data_layer=hive_ctx.get("data_layer", ""),
            data_domain=hive_ctx.get("data_domain", ""),
            data_source=hive_ctx.get("data_source", ""),
            data_origin=hive_ctx.get("data_origin", ""),
            asset_type=hive_ctx.get("asset_type", ""),
            resolution=hive_ctx.get("resolution", ""),
            size_bytes=db_size,
            last_modified=db_mtime,
            path=str(db_file),
            status="locked" if is_locked else "unreadable",
        )
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


# ---------- Gather + filter ----------


def _gather_all_units(errors: list[str] | None = None) -> list[DataUnit]:
    """Discover every storage object across every configured filesystem root.

    `Storage` reflects the pfeed storage class (LOCAL/CACHE for file-based,
    DUCKDB/LANCEDB for database-backed). `Backend` is the on-disk format.
    Database storages are discovered by scanning *every* configured root since
    a sqlite/duckdb/lancedb file can live under either data_path or cache_path
    (or anywhere, if the user has configured custom paths).
    """
    from pfeed.config import get_config

    cfg = get_config()
    data_root = Path(cfg.data_path)
    cache_root = Path(cfg.cache_path)
    labeled_roots = [("LOCAL", data_root), ("CACHE", cache_root)]

    units: list[DataUnit] = []
    units.extend(_discover_file_based("LOCAL", data_root))
    units.extend(_discover_file_based("CACHE", cache_root))
    units.extend(_discover_sqlite(labeled_roots, errors=errors))
    units.extend(_discover_duckdb(labeled_roots, errors=errors))
    return units


def _print_errors(errors: list[str]):
    if not errors:
        return
    from rich.console import Console

    Console(stderr=True).print("\n".join(f"[yellow]! {e}[/yellow]" for e in errors))


def _apply_filters(units: list[DataUnit], **filters: str | None) -> list[DataUnit]:
    def keep(u: DataUnit) -> bool:
        for attr, val in filters.items():
            if not val:
                continue
            v = val.upper()
            if attr == "symbol":
                if v not in {s.upper() for s in u.symbols}:
                    return False
            elif attr == "storage":
                # storage filter matches either the storage column or the backend column,
                # so `--storage DUCKDB` (backend) and `--storage LOCAL` (root) both work.
                if u.storage.upper() != v and u.backend.upper() != v:
                    return False
            else:
                actual = getattr(u, attr, "") or ""
                if str(actual).upper() != v:
                    return False
        return True

    return [u for u in units if keep(u)]


def _hierarchical_sort_key(u: DataUnit) -> tuple[str, ...]:
    return (
        u.storage,
        u.data_layer,
        u.env,
        u.data_source,
        u.data_origin,
        u.asset_type,
        u.resolution,
        u.symbol_cell,
    )


# ---------- Rendering ----------


def _console():
    from rich.console import Console

    return Console()


def _shared_columns(units: list[DataUnit]) -> dict[str, str]:
    """Columns where every row has the same non-empty value — collapse to caption."""
    if not units:
        return {}
    shared: dict[str, str] = {}
    for attr in COLLAPSIBLE_COLUMNS:
        vals = {getattr(u, attr) or "" for u in units}
        if len(vals) == 1:
            (v,) = vals
            if v:
                shared[attr] = v
    return shared


def _build_overview_table(units: list[DataUnit]):
    from rich import box
    from rich.table import Table

    t = Table(title="pfeed data — storage overview", box=box.SIMPLE_HEAVY)
    t.add_column("Storage", style="cyan")
    t.add_column("Backend", style="magenta")
    t.add_column("Layer")
    t.add_column("Domain")
    t.add_column("Datasets", justify="right")
    t.add_column("Products", justify="right")
    t.add_column("Dates", justify="right")
    t.add_column("Latest")
    t.add_column("Updated")
    t.add_column("Size", justify="right")

    groups: dict[tuple[str, str, str, str], list[DataUnit]] = {}
    for u in units:
        groups.setdefault(
            (u.storage, u.backend, u.data_layer, u.data_domain), []
        ).append(u)

    total_units = total_products = total_dates = total_size = 0
    backends_seen: set[str] = set()
    for key, group in sorted(groups.items()):
        storage, backend, layer, domain = key
        backends_seen.add(backend)
        all_symbols: set[str] = {s for u in group for s in u.symbols}
        all_dates: set[datetime.date] = {d for u in group for d in u.dates}
        size = sum(u.size_bytes for u in group)
        latest_date = max(all_dates) if all_dates else None
        latest_mtimes = [u.last_modified for u in group if u.last_modified]
        latest_mtime = max(latest_mtimes) if latest_mtimes else None
        t.add_row(
            storage,
            backend,
            layer or "—",
            domain or "—",
            f"{len(group):,}",
            f"{len(all_symbols):,}" if all_symbols else "—",
            f"{len(all_dates):,}" if all_dates else "—",
            str(latest_date) if latest_date else "—",
            _fmt_relative_time(latest_mtime),
            _fmt_size(size),
        )
        total_units += len(group)
        total_products += len(all_symbols)
        total_dates += len(all_dates)
        total_size += size

    if groups:
        t.caption = (
            f"{len(groups)} rows · {len(backends_seen)} backends · "
            f"{total_units:,} datasets · {total_products:,} products · "
            f"{total_dates:,} date-partitions · {_fmt_size(total_size)}"
            "\nRun `pfeed data ls` for per-dataset detail."
        )
    else:
        t.caption = "No data found in any configured storage."
    return t


def _build_ls_table(units: list[DataUnit]):
    from rich import box
    from rich.table import Table

    shared = _shared_columns(units)
    visible_collapsible = [c for c in COLLAPSIBLE_COLUMNS if c not in shared]

    t = Table(title="pfeed data ls", box=box.SIMPLE_HEAVY)
    for attr in visible_collapsible:
        t.add_column(COLUMN_LABELS[attr])
    if "data_source" not in shared:
        pass  # already included as "Source"
    # Symbol always shown — it's the primary content
    t.add_column("Symbol")
    t.add_column("Dates", justify="right")
    t.add_column("Range")
    t.add_column("Updated")
    t.add_column("Size", justify="right")

    for u in units:
        row: list[str] = []
        for attr in visible_collapsible:
            if attr == "data_source":
                row.append(u.source_cell)
            else:
                row.append(getattr(u, attr, "") or "—")
        row.append(u.symbol_cell)
        row.append(f"{len(u.dates):,}" if u.dates else "—")
        row.append(u.date_range)
        row.append(_fmt_relative_time(u.last_modified))
        row.append(_fmt_size(u.size_bytes))
        t.add_row(*row)

    if shared:
        labels = " · ".join(
            f"{COLUMN_LABELS[a]}={shared[a]}"
            for a in COLLAPSIBLE_COLUMNS
            if a in shared
        )
        t.caption = f"All rows share: {labels}"
    return t


def _to_json_dicts(units: list[DataUnit]) -> list[dict[str, Any]]:
    out = []
    for u in units:
        d = asdict(u)
        d["dates"] = [str(x) for x in u.dates]
        d["last_modified"] = u.last_modified.isoformat() if u.last_modified else None
        out.append(d)
    return out


# ---------- Shared filter decorator for ls / show ----------


def _filter_options(f):
    f = click.option(
        "--resolution", "-r", default=None, help="Filter by resolution (e.g. 1_MINUTE)"
    )(f)
    f = click.option("--symbol", default=None, help="Filter by product symbol")(f)
    f = click.option(
        "--asset-type", "asset_type", default=None, help="Filter by asset type"
    )(f)
    f = click.option("--env", default=None, help="Filter by env (BACKTEST/PAPER/LIVE)")(
        f
    )
    f = click.option(
        "--source", default=None, help="Filter by data source (e.g. BYBIT)"
    )(f)
    f = click.option(
        "--domain", "-d", default=None, help="Filter by data domain (e.g. MARKET_DATA)"
    )(f)
    f = click.option(
        "--layer", "-l", default=None, help="Filter by data layer (RAW/CLEANED/CURATED)"
    )(f)
    f = click.option(
        "--storage", "-s", default=None, help="Filter by storage (LOCAL/CACHE/DUCKDB)"
    )(f)
    return f


def _collect(
    storage,
    layer,
    domain,
    source,
    env,
    asset_type,
    symbol,
    resolution,
    errors: list[str] | None = None,
):
    units = _gather_all_units(errors=errors)
    return _apply_filters(
        units,
        storage=storage,
        data_layer=layer,
        data_domain=domain,
        data_source=source,
        env=env,
        asset_type=asset_type,
        resolution=resolution,
        symbol=symbol,
    )


# ---------- Click commands ----------


@click.group(invoke_without_command=True)
@click.pass_context
def data(ctx: click.Context):
    """Inspect pfeed data across all configured storages.

    Run without a subcommand for a one-screen storage overview.
    Use `pfeed data ls` for the per-dataset table or
    `pfeed data tree` for a hierarchical view.
    """
    if ctx.invoked_subcommand is None:
        errors: list[str] = []
        units = _gather_all_units(errors=errors)
        _console().print(_build_overview_table(units))
        _print_errors(errors)


@data.command("ls")
@_filter_options
@click.option("--limit", default=50, show_default=True, type=int)
@click.option(
    "--all", "show_all", is_flag=True, help="Show everything (ignore --limit)"
)
@click.option(
    "--sort",
    "sort_by",
    default="hierarchy",
    show_default=True,
    type=click.Choice(["hierarchy", "source", "symbol", "dates", "size", "updated"]),
)
@click.option("--json", "as_json", is_flag=True, help="Emit JSON instead of a table")
def list_data(
    storage,
    layer,
    domain,
    source,
    env,
    asset_type,
    symbol,
    resolution,
    limit,
    show_all,
    sort_by,
    as_json,
):
    """List datasets/tables (dates collapsed). Columns shared across all rows auto-hide."""
    errors: list[str] = []
    units = _collect(
        storage,
        layer,
        domain,
        source,
        env,
        asset_type,
        symbol,
        resolution,
        errors=errors,
    )
    sorters = {
        "hierarchy": _hierarchical_sort_key,
        "source": lambda u: (u.data_source, u.symbol_cell),
        "symbol": lambda u: u.symbol_cell,
        "dates": lambda u: -len(u.dates),
        "size": lambda u: -u.size_bytes,
        "updated": lambda u: -(u.last_modified.timestamp() if u.last_modified else 0),
    }
    units.sort(key=sorters[sort_by])

    total = len(units)
    truncated = (not show_all) and total > limit
    display = units[:limit] if truncated else units

    if as_json:
        click.echo(json.dumps(_to_json_dicts(display), indent=2))
        return

    if total == 0:
        _console().print("[yellow]No data matched.[/yellow]")
        return

    _console().print(_build_ls_table(display))
    if truncated:
        _console().print(
            f"[dim]Showing {limit} of {total}. Use --all, or add filters to narrow.[/dim]"
        )
    _print_errors(errors)


@data.command()
@click.option("--storage", "-s", default=None, help="Limit tree to one storage")
def tree(storage: str | None):
    """Tree view of everything pfeed has, reflecting on-disk layout.

    File-based backends (parquet, delta) sit inside the hive tree.
    Database-backed files (sqlite, duckdb, lancedb) are physical files at the storage
    root, so they appear as siblings of the hive tree, not nested inside it.
    """
    from rich.tree import Tree

    from pfeed.config import get_config

    errors: list[str] = []
    units = _gather_all_units(errors=errors)
    if storage:
        units = _apply_filters(units, storage=storage)

    cfg = get_config()
    storage_roots = {"LOCAL": Path(cfg.data_path), "CACHE": Path(cfg.cache_path)}

    root = Tree("[bold]pfeed data[/bold]")

    by_storage: dict[str, list[DataUnit]] = {}
    for u in units:
        by_storage.setdefault(u.storage, []).append(u)

    HIVE_BRANCH_LEVELS = (
        "env",
        "data_layer",
        "data_domain",
        "data_source",
        "data_origin",
        "asset_type",
        "resolution",
    )

    for storage_label in sorted(by_storage.keys()):
        storage_units = by_storage[storage_label]
        storage_path = storage_roots.get(storage_label)
        label = f"[cyan]{storage_label}[/cyan]"
        if storage_path:
            label += f"  [dim]{storage_path}[/dim]"
        storage_node = root.add(label)

        db_units = [
            u for u in storage_units if u.backend in {"sqlite", "duckdb", "lancedb"}
        ]
        hive_units = [
            u for u in storage_units if u.backend not in {"sqlite", "duckdb", "lancedb"}
        ]

        # Database-backed files: sibling of hive tree at the storage root.
        by_db_file: dict[str, list[DataUnit]] = {}
        for u in db_units:
            by_db_file.setdefault(u.path.split("::")[0], []).append(u)
        for db_path, db_us in sorted(by_db_file.items()):
            try:
                rel = Path(db_path).relative_to(storage_path) if storage_path else None
                display = str(rel) if rel else Path(db_path).name
            except ValueError:
                display = Path(db_path).name
            db_node = storage_node.add(
                f"[magenta]{display}[/magenta]  [dim]({db_us[0].backend})[/dim]"
            )
            for u in db_us:
                if u.status:
                    db_node.add(
                        f"{u.symbol_cell}  [dim]status: {u.status}, "
                        f"{_fmt_size(u.size_bytes)}, updated {_fmt_relative_time(u.last_modified)}[/dim]"
                    )
                else:
                    schema_table = u.path.split("::", 1)[1] if "::" in u.path else ""
                    prefix = f"{schema_table}  " if schema_table else ""
                    db_node.add(
                        f"{prefix}{u.symbol_cell}  [dim]({len(u.dates)} dates, "
                        f"{u.date_range}, {_fmt_size(u.size_bytes)}, "
                        f"updated {_fmt_relative_time(u.last_modified)})[/dim]"
                    )

        # File-based: parquet + delta under the hive tree.
        if hive_units:
            nested: dict[Any, Any] = {}
            for u in hive_units:
                node = nested
                for key in HIVE_BRANCH_LEVELS:
                    val = getattr(u, key) or "—"
                    node = node.setdefault(val, {})
                node.setdefault("__leaves__", []).append(u)

            def walk(d: dict[Any, Any], parent):
                for k in sorted(kk for kk in d if kk != "__leaves__"):
                    sub = parent.add(f"[cyan]{k}[/cyan]")
                    walk(d[k], sub)
                for u in d.get("__leaves__", []):
                    parent.add(
                        f"{u.symbol_cell}  [dim]({len(u.dates)} dates, "
                        f"{u.date_range}, {_fmt_size(u.size_bytes)}, "
                        f"updated {_fmt_relative_time(u.last_modified)}, {u.backend})[/dim]"
                    )

            walk(nested, storage_node)

    _console().print(root)
    _print_errors(errors)
