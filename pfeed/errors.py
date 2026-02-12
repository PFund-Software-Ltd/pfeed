class PFeedError(Exception):
    pass


class DataNotFoundError(PFeedError):
    """Raised when requested data is not found in any storage backend"""
    pass

