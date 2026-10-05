"""Tests of `pfeed ducklake|deltalake|iceberg optimize|vacuum`."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from pfeed.io.table_io import TableIO

import datetime

import click
import pyarrow as pa
import pytest
from click.testing import CliRunner

from pfeed.cli.commands.table_io import deltalake, ducklake, iceberg
from pfeed.io.base_io import DatasetKey
from pfeed.io.deltalake_io import DeltaLakeIO
from pfeed.io.ducklake_io import DuckLakeIO
from pfeed.io.iceberg_io import IcebergIO

KEY = DatasetKey(namespace={'env': 'BACKTEST'}, name={'resolution': '1t'}, partition_by=('date',))
D1 = datetime.date(2025, 1, 1)
COMMANDS = [(ducklake, DuckLakeIO), (deltalake, DeltaLakeIO), (iceberg, IcebergIO)]


@pytest.fixture(params=COMMANDS, ids=[command.name for command, _ in COMMANDS])
def command_and_io_class(request) -> tuple[click.Group, type[TableIO]]:
    return request.param


def test_group_name_is_default_dir_name(command_and_io_class: tuple[click.Group, type[TableIO]]):
    """The --base-path help shows <data_path>/<group name> as the default, which is the IO's DEFAULT_DIR_NAME."""
    command, io_class = command_and_io_class
    assert command.name == io_class.DEFAULT_DIR_NAME


def test_missing_base_path_raises(command_and_io_class: tuple[click.Group, type[TableIO]], tmp_path: Path):
    """Maintaining a lake that doesn't exist fails, instead of creating an empty one."""
    command, _ = command_and_io_class
    base_path = tmp_path / 'missing'

    result = CliRunner().invoke(command, ['--base-path', str(base_path), 'optimize'])

    assert result.exit_code != 0
    assert 'No ' in result.output
    assert not base_path.exists()


def test_optimize_and_vacuum(command_and_io_class: tuple[click.Group, type[TableIO]], tmp_path: Path):
    """optimize, then vacuum: a dry run by default (deletes nothing), --no-dry-run deletes the replaced files.

    The partition is written twice without data inlining, so its first parquet file is replaced.
    """
    command, io_class = command_and_io_class
    io = DuckLakeIO(base_path=str(tmp_path), data_inlining_row_limit=0) if io_class is DuckLakeIO else io_class(base_path=str(tmp_path))
    data = pa.table({'ts': [1], 'date': [D1]})
    io.write(KEY, data, partitions={(D1,): {}})
    io.write(KEY, data, partitions={(D1,): {}})
    runner = CliRunner()
    base_path = ['--base-path', str(tmp_path)]

    result = runner.invoke(command, [*base_path, 'optimize'])
    assert result.exit_code == 0, result.output

    files_before = set(tmp_path.rglob('*.parquet'))
    result = runner.invoke(command, [*base_path, 'vacuum', '-h', '0'])
    assert result.exit_code == 0, result.output
    assert 'dry run' in result.output
    assert 'Going to expire' in result.output
    assert set(tmp_path.rglob('*.parquet')) == files_before

    for _ in range(2):  # DuckLake deletes on the second vacuum, see DuckLakeIO.vacuum()
        result = runner.invoke(command, [*base_path, 'vacuum', '-h', '0', '--no-dry-run'])
        assert result.exit_code == 0, result.output
    assert set(tmp_path.rglob('*.parquet')) < files_before  # vacuum adds commit files, so only data files
    lf, metadata = io.read(KEY)
    assert lf is not None
    assert metadata == {(D1,): {}}
    assert lf.collect().to_arrow().select(['ts', 'date']).equals(data)
