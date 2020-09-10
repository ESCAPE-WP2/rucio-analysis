import logging
import sys

class Logger():
    def __init__(self, name='root', level='DEBUG', 
    format='%(asctime)s %(levelname)s\t%(message)s'):
        self._name = name
        self.logger = self._get()
        self.logger.setLevel(logging.DEBUG)

        # console handler
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(level)
        formatter = logging.Formatter(format)
        ch.setFormatter(fmt=formatter)
        self.logger.addHandler(ch)
        

    def _get(self):
        return logging.getLogger(self._name)





