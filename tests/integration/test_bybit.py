import datetime

import pytest

from pfeed.feeds import BybitFeed
from pfeed.storages.base_storage import BaseStorage
from pfeed.const.enums import DataRawLevel


@pytest.fixture
def feed(request):
    if hasattr(request, 'param'):
        return BybitFeed(**request.param)
    else:
        return BybitFeed()


class TestDownload:
    @staticmethod
    def get_params_for_download():
        return {
            'products': ['BTC_USDT_PERP'],
            'data_type': 'tick',
            'start_date': '2024-01-01',
            'end_date': '2024-01-01',
            'to_storage': 'local',
        }
    
    def extract_downloaded_data(self, feed, pdt, data_type, date, to_storage, raw_level, filename_prefix: str='', filename_suffix: str='') -> BaseStorage | None:
        from pfeed.etl import extract_data
        from pfund.datas.resolution import Resolution
        data_model = feed.create_market_data_model(
            product=pdt, 
            resolution=Resolution(data_type), 
            date=datetime.datetime.strptime(date, '%Y-%m-%d').date(), 
            raw_level=raw_level,
            filename_prefix=filename_prefix, 
            filename_suffix=filename_suffix
        )
        return extract_data(data_model, storage=to_storage)

    @pytest.mark.parametrize('feed', [
        {'data_tool': 'pandas', 'use_ray': False, 'use_prefect': False, 'pipeline_mode': False},
        {'data_tool': 'pandas', 'use_ray': True, 'use_prefect': True, 'pipeline_mode': False},
        {'data_tool': 'polars', 'use_ray': False, 'use_prefect': True, 'pipeline_mode': False},
        {'data_tool': 'polars', 'use_ray': True, 'use_prefect': False, 'pipeline_mode': False},
    ], indirect=True)
    def test_download(self, feed):
        raw_level_by_data_tool_and_use_ray = {
            ('pandas', False): 'normalized',
            ('polars', True): 'cleaned',
            ('pandas', True): 'original',
            ('polars', False): 'original',
        }
        raw_level = DataRawLevel[raw_level_by_data_tool_and_use_ray[(feed.data_tool.name.value.lower(), feed._use_ray)].upper()]
        params = self.get_params_for_download()
        pdt, data_type, date, to_storage = params['products'][0], params['data_type'], params['start_date'], params['to_storage']
        filename_prefix = self.test_download.__name__
        filename_suffix = raw_level.name.lower()
        storage: BaseStorage | None = self.extract_downloaded_data(feed, pdt, data_type, date, to_storage, raw_level, filename_prefix=filename_prefix, filename_suffix=filename_suffix)
        # only download if the data is not already downloaded
        if storage is None or not storage.exists():
            feed.download(**params, raw_level=raw_level.name, filename_prefix=filename_prefix, filename_suffix=filename_suffix)
            storage: BaseStorage | None = self.extract_downloaded_data(feed, pdt, data_type, date, to_storage, raw_level, filename_prefix=filename_prefix, filename_suffix=filename_suffix)
            assert storage is not None
            assert storage.exists()
        df = feed.data_tool.read_parquet(storage.file_path, storage=storage.name.value)
        assert not feed.data_tool.is_empty(df)
        cols_after_cleaned = ['ts', 'product', 'resolution', 'side', 'price', 'volume']
        if raw_level == 'normalized':
            assert set(df.columns) == set(cols_after_cleaned + ['symbol', 'tickDirection', 'trdMatchID', 'grossValue', 'homeNotional', 'foreignNotional'])
        elif raw_level == 'cleaned':
            assert set(df.columns) == set(cols_after_cleaned)
        elif raw_level == 'original':
            original_cols = ['timestamp', 'symbol', 'side', 'price', 'size', 'tickDirection', 'trdMatchID', 'grossValue', 'homeNotional', 'foreignNotional']
            assert set(df.columns) == set(original_cols)
            
    @pytest.mark.parametrize('feed', [
        {'data_tool': 'pandas', 'use_ray': False, 'use_prefect': False, 'pipeline_mode': True},
    ], indirect=True)
    def test_download_with_custom_transforms(self, feed):
        assert feed._pipeline_mode
        pdt, data_type, date, raw_level, storage = 'BTC_USDT_PERP', 'tick', '2024-01-01', 'normalized', 'local'
        raw_level = DataRawLevel[raw_level.upper()]
        filename_prefix = self.test_download_with_custom_transforms.__name__
        filename_suffix = raw_level.name.lower()
        storage: BaseStorage | None = self.extract_downloaded_data(feed, pdt, data_type, date, storage, raw_level, filename_prefix=filename_prefix, filename_suffix=filename_suffix)
        if storage is None or not storage.exists():
            # only download if the data is not already downloaded
            (
                feed
                .download(products=[pdt], data_type=data_type, start_date=date, end_date=date, raw_level=raw_level.name, filename_prefix=filename_prefix, filename_suffix=filename_suffix)
                .transform(lambda df: df.assign(ma5=df['price'].rolling(window=5).mean()))
                .transform(lambda df: df.assign(price_pct_change=df['price'].pct_change() * 100))
                .run()
            )
            storage: BaseStorage | None = self.extract_downloaded_data(feed, pdt, data_type, date, storage, raw_level, filename_prefix=filename_prefix, filename_suffix=filename_suffix)
            assert storage is not None
            assert storage.exists()
        df = feed.data_tool.read_parquet(storage.file_path, storage=storage.name.value)
        # Verify transformations were applied
        assert 'ma5' in df.columns
        assert 'price_pct_change' in df.columns
    
    # TODO: e.g. resample to 1-minute
    def test_download_with_resample(self, feed):
        pass
