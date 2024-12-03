import pytest
import pandas as pd
import dask.dataframe as dd
import io

from dask.dataframe.utils import assert_eq
# from pfeed.data_tools.data_tool_dask import read_parquet, concat, sort_by_ts, is_empty


pytestmark = pytest.mark.skip(reason="Dask is not supported yet")


@pytest.fixture
def sample_df():
    pdf = pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': ['a', 'b', 'c']
    })
    return dd.from_pandas(pdf, npartitions=1)

@pytest.fixture
def sample_dfs():
    df1 = dd.from_pandas(pd.DataFrame({
        'col1': [1, 2],
        'col2': ['a', 'b']
    }), npartitions=1)
    df2 = dd.from_pandas(pd.DataFrame({
        'col1': [3, 4],
        'col2': ['c', 'd']
    }), npartitions=1)
    return [df1, df2]

@pytest.fixture
def ts_df():
    pdf = pd.DataFrame({
        'ts': pd.to_datetime(['2024-01-01', '2024-01-03', '2024-01-02']),
        'value': [1, 3, 2]
    })
    return dd.from_pandas(pdf, npartitions=1)

@pytest.fixture
def parquet_bytes(sample_df):
    buffer = io.BytesIO()
    sample_df.compute().to_parquet(buffer)
    return buffer.getvalue()

def test_read_parquet_from_bytes(sample_df, parquet_bytes):
    result = read_parquet(parquet_bytes, storage='local')
    assert_eq(result, sample_df, check_divisions=False)

def test_read_parquet_from_single_path(tmp_path, sample_df):
    file_path = tmp_path / "test.parquet"
    sample_df.compute().to_parquet(file_path)
    
    result = read_parquet(str(file_path), storage='local')
    assert_eq(result, sample_df, check_divisions=False)

def test_read_parquet_from_multiple_paths(tmp_path, sample_df):
    file_paths = []
    for i in range(2):
        file_path = tmp_path / f"test_{i}.parquet"
        sample_df.compute().to_parquet(file_path)
        file_paths.append(str(file_path))
    
    result = read_parquet(file_paths, storage='local')
    expected = dd.concat([sample_df, sample_df], ignore_index=True)
    assert_eq(result, expected, check_divisions=False)

def test_concat_with_index_reset(sample_dfs):
    result = concat(sample_dfs)
    expected = dd.from_pandas(pd.DataFrame({
        'col1': [1, 2, 3, 4],
        'col2': ['a', 'b', 'c', 'd']
    }), npartitions=1)
    assert_eq(result, expected, check_divisions=False)

def test_sort_by_ts(ts_df):
    result = sort_by_ts(ts_df)
    expected = dd.from_pandas(pd.DataFrame({
        'ts': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
        'value': [1, 2, 3]
    }), npartitions=1)
    assert_eq(result, expected, check_divisions=False)

def test_sort_by_ts_missing_column():
    df_without_ts = dd.from_pandas(
        pd.DataFrame({'value': [1, 2, 3]}),
        npartitions=1
    )
    with pytest.raises(KeyError):
        sort_by_ts(df_without_ts).compute()

def test_is_empty_with_empty_df():
    empty_df = dd.from_pandas(
        pd.DataFrame(),
        npartitions=1
    )
    assert is_empty(empty_df) is True

def test_is_empty_with_data():
    non_empty_df = dd.from_pandas(
        pd.DataFrame({'col': [1, 2, 3]}),
        npartitions=1
    )
    assert is_empty(non_empty_df) is False

def test_is_empty_with_columns_no_rows():
    df_with_columns = dd.from_pandas(
        pd.DataFrame(columns=['col1', 'col2']),
        npartitions=1
    )
    assert is_empty(df_with_columns) is True