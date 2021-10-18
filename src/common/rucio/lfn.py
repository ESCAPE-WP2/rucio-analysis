class LFN():
    def __init__(self, abspath):
        self._abspath = abspath

    @property
    def abspath(self):
        return self._abspath
