from pfeed.feeds.base_feed import BaseFeed


# TODO: need to unify the filtering/querying syntax
class ScreenerFeed(BaseFeed):
    data_domain = 'screener_data'


    # TODO: need to think about how to structure it, it might not be time-based
    def retrieve(self):
        '''retrieve fundamental data in storage to build a dataset for screening'''
        pass
    
    def fetch(self):
        '''fetch data from the external screener'''
        pass