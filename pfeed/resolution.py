from pfund.datas.resolution import Resolution as BaseResolution


__all__ = ['ExtendedResolution']


class ExtendedResolution(BaseResolution):
    '''Supports normal resolutions such as '1m', '1h', and the raw ones such as 'r1m', 'r1h' where r stands for raw '''    
    def __init__(self, resolution: str):
        self._is_raw = True if resolution.startswith('r') else False
        super().__init__(resolution[1:] if self._is_raw else resolution)
    
    def is_raw(self):
        return self._is_raw
    
    def is_equal(self, other):
        """
        Returns True if the resolution is the same, regardless of whether it is raw or not.
        It is a soft comparison, so '1m' == 'r1m'
        """
        if not isinstance(other, ExtendedResolution):
            return NotImplemented
        return self._value() == other._value()
    
    def is_ge(self, other):
        """
        Returns True if the resolution is greater than or equal to the other resolution, regardless of whether it is raw or not.
        It is a soft comparison, so '1m' >= 'r1m'
        """
        if not isinstance(other, ExtendedResolution):
            return NotImplemented
        return self._value() < other._value() or self.is_equal(other)
    
    def is_le(self, other):
        """
        Returns True if the resolution is less than or equal to the other resolution, regardless of whether it is raw or not.
        It is a soft comparison, so '1m' <= 'r1m'
        """
        if not isinstance(other, ExtendedResolution):
            return NotImplemented
        return self._value() > other._value() or self.is_equal(other)

    def __str__(self):
        return 'RAW_' + super().__str__() if self._is_raw else super().__str__()
    
    def __repr__(self):
        return 'r' + super().__repr__() if self._is_raw else super().__repr__()
    
    def __eq__(self, other):
        if not isinstance(other, ExtendedResolution):
            return NotImplemented
        return self._value() == other._value() and self._is_raw == other._is_raw
    
    def __ge__(self, other):
        if not isinstance(other, ExtendedResolution):
            return NotImplemented
        return self._value() < other._value() or self == other

    def __le__(self, other):
        if not isinstance(other, ExtendedResolution):
            return NotImplemented
        return self._value() > other._value() or self == other