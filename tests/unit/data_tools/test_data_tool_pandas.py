import pytest
import pandas as pd
import io

from pfeed.data_tools.data_tool_pandas import read_parquet, concat, sort_by_ts, is_empty

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': ['a', 'b', 'c']
    })

@pytest.fixture
def sample_dfs():
    df1 = pd.DataFrame({
        'col1': [1, 2],
        'col2': ['a', 'b']
    })
    df2 = pd.DataFrame({
        'col1': [3, 4],
        'col2': ['c', 'd']
    })
    return [df1, df2]

@pytest.fixture
def ts_df():
    return pd.DataFrame({
        'date': pd.to_datetime(['2024-01-01', '2024-01-03', '2024-01-02']),
        'value': [1, 3, 2]
    })

@pytest.fixture
def parquet_bytes(sample_df):
    buffer = io.BytesIO()
    sample_df.to_parquet(buffer)
    return buffer.getvalue()

def test_read_parquet_from_bytes(sample_df, parquet_bytes):
    result = read_parquet(parquet_bytes, storage='local')
    pd.testing.assert_frame_equal(result, sample_df)

def test_read_parquet_from_single_path(tmp_path, sample_df):
    # Create a temporary parquet file
    file_path = tmp_path / "test.parquet"
    sample_df.to_parquet(file_path)
    
    result = read_parquet(str(file_path), storage='local')
    pd.testing.assert_frame_equal(result, sample_df)

def test_read_parquet_from_multiple_paths(tmp_path, sample_df):
    # Create multiple temporary parquet files with same schema
    file_paths = []
    for i in range(2):
        file_path = tmp_path / f"test_{i}.parquet"
        sample_df.to_parquet(file_path)
        file_paths.append(str(file_path))
    
    result = read_parquet(file_paths, storage='local')
    expected = pd.concat([sample_df, sample_df], ignore_index=True)
    pd.testing.assert_frame_equal(result, expected)

def test_concat_with_index_reset(sample_dfs):
    result = concat(sample_dfs)
    expected = pd.DataFrame({
        'col1': [1, 2, 3, 4],
        'col2': ['a', 'b', 'c', 'd']
    })
    pd.testing.assert_frame_equal(result, expected)

def test_sort_by_ts(ts_df):
    result = sort_by_ts(ts_df)
    expected = pd.DataFrame({
        'date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
        'value': [1, 2, 3]
    })
    pd.testing.assert_frame_equal(result, expected)

def test_sort_by_ts_missing_column():
    df_without_ts = pd.DataFrame({'value': [1, 2, 3]})
    with pytest.raises(KeyError):
        sort_by_ts(df_without_ts)

def test_is_empty_with_empty_df():
    empty_df = pd.DataFrame()
    assert is_empty(empty_df) is True

def test_is_empty_with_data():
    non_empty_df = pd.DataFrame({'col': [1, 2, 3]})
    assert is_empty(non_empty_df) is False

def test_is_empty_with_columns_no_rows():
    df_with_columns = pd.DataFrame(columns=['col1', 'col2'])
    assert is_empty(df_with_columns) is True