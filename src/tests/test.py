import abc
import inspect
import time

class Test():
    """ Base class for all tests.
    """
    def __init__(self, logger):
        self.logger = logger
        self.start = None
        self.end = None

        self.logger.debug("Constructing instance of {}()".format(
            type(self).__name__))

    
    def __del__(self):
        self.logger.debug("Deconstructing instance of {}()".format(
            type(self).__name__))


    @abc.abstractmethod
    def run(self):
        """ Entry point for all derived test classes. """
        self.logger.info("Executing {}.{}()".format(
            type(self).__name__, inspect.stack()[0][3]))


    def tic(self):
        """ Start a timer. """
        self.start = time.time()


    def toc(self):
        """ End a timer. """
        self.end = time.time()


    @property
    def elapsed(self): 
        """ Get the elapsed time on the timer. """
        if self.start is None:
            return 0
        elif self.end is None:
            return round(time.time() - self.start, 3)
        else:
            return round(self.end - self.start, 3)
