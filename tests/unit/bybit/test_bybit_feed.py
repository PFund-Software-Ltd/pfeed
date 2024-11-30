import pytest
import pandas as pd
from pfeed.feeds import BybitFeed


@pytest.fixture
def feed():
    return BybitFeed()


class TestDownload:
    def test_download_functions_exist(self):
        from pfeed.feeds import BybitFeed
        dicts = BybitFeed.__dict__
        assert 'download' in dicts
        assert '_execute_download' in dicts
    
    def test_dataflows_cleared_before_download(self, feed, mocker):
        spy = mocker.spy(feed, '_clear_current_dataflows')
        mocker.patch.object(feed, 'run')
        feed.download('BTC_USDT_PERP')
        # _clear_current_dataflows is called twice:
        # once before the download operation is added
        # and once after the load operation is added
        assert spy.call_count == 2

    def test_execute_download_return_none_if_download_fails(self, feed, mocker):
        mocker.patch.object(feed.data_source, 'download_market_data', return_value=None)
        data_model = mocker.Mock(product='BTC_USDT_PERP', date='2024-01-01')
        result = feed._execute_download(data_model)
        assert result is None
        
    def test_execute_download_return_df_if_download_succeeds(self, feed, mocker):
        mocker.patch.object(feed.data_source, 'download_market_data', return_value=b"column1,column2\n1,2\n3,4\n5,6")
        data_model = mocker.Mock(product='BTC_USDT_PERP', date='2024-01-01')
        result = feed._execute_download(data_model)
        assert isinstance(result, pd.DataFrame)
        pd.testing.assert_frame_equal(result, pd.DataFrame({'column1': [1, 3, 5], 'column2': [2, 4, 6]}))


class TestNormalizeRawData:
    def test_column_renaming(self, feed):
        """Test if columns are renamed correctly"""
        df = pd.DataFrame({
            'timestamp': [1671580800, 1671580801],  # milliseconds
            'size': [100, 200],
            'side': ['Buy', 'Sell']
        })
        result = feed._normalize_raw_data(df)
        assert 'ts' in result.columns  # timestamp -> ts
        assert 'volume' in result.columns  # size -> volume
        assert set(result.columns) == {'ts', 'volume', 'side'}

    def test_millisecond_timestamp(self, feed):
        """Test timestamp conversion when input is in milliseconds"""
        timestamps = [1671580800123, 1671580801123]
        df = pd.DataFrame({
            'timestamp': timestamps,  # milliseconds
            'size': [100, 200],
            'side': ['Buy', 'Buy']
        })
        result = feed._normalize_raw_data(df)
        expected_ts = pd.Series(pd.to_datetime(timestamps, unit='ms'))
        pd.testing.assert_series_equal(result['ts'], expected_ts, check_names=False)

    def test_second_timestamp(self, feed):
        """Test timestamp conversion when input is in seconds"""
        timestamps = [1671580800, 1671580801]
        df = pd.DataFrame({
            'timestamp': timestamps,  # seconds
            'size': [100, 200],
            'side': ['Buy', 'Buy']
        })
        result = feed._normalize_raw_data(df)
        expected_ts = pd.Series(pd.to_datetime(timestamps, unit='s'))
        pd.testing.assert_series_equal(result['ts'], expected_ts, check_names=False)

    def test_reverse_order_detection(self, feed):
        """Test detection and correction of reverse-ordered data"""
        df = pd.DataFrame({
            'timestamp': [1671580802, 1671580801, 1671580800],  # decreasing
            'size': [300, 200, 100],
            'side': ['Buy', 'Sell', 'Buy']
        })
        result = feed._normalize_raw_data(df)
        # Check if timestamps are now in ascending order
        assert result['ts'].is_monotonic_increasing
        # Check if associated data was also reversed correctly
        assert result['volume'].tolist() == [100, 200, 300]
        assert result['side'].tolist() == [1, -1, 1]

    def test_side_mapping(self, feed):
        """Test mapping of Buy/Sell to 1/-1"""
        df = pd.DataFrame({
            'timestamp': [1671580800, 1671580801, 1671580802],
            'size': [100, 200, 300],
            'side': ['Buy', 'Sell', 'Buy']
        })
        result = feed._normalize_raw_data(df)
        assert (result['side'] == [1, -1, 1]).all()
