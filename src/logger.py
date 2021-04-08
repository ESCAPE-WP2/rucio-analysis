import logging
import sys


class Logger:
    def __init__(
        self,
        name="root",
        level="DEBUG",
        fmt="%(asctime)s [%(name)s] %(module)20s %(levelname)5s %(process)d\t" +
        "%(message)s",
        add_ch=True
    ):
        self._fmt = fmt
        self._name = name
        self._level = level

        # Create Formatter object with desired logging format.
        #
        self.formatter = logging.Formatter(self.fmt)

        # Logger set to capture ALL logging events, handlers then further restrict
        # this with their own levels.
        #
        self.get().setLevel("DEBUG")

        # Stop child loggers propagating to parents, prevents double logging.
        #
        self.get().propagate = False

        # Clear existing handlers and add new ones as requested.
        #
        self._clearHandlers()
        if add_ch:
            self._addConsoleHandler()

    def _clearHandlers(self):
        self.get().handlers = []

    def _addConsoleHandler(self):
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(self.level)
        ch.setFormatter(fmt=self.formatter)
        self.get().addHandler(ch)

    @property
    def config(self):
        """ Return a reference to the config. """
        return self.get().config

    @property
    def fmt(self):
        """ Getter for format of this logger. """
        return self._fmt

    def get(self):
        """ Return a reference to this logger. """
        return logging.getLogger(self.name)

    @property
    def level(self):
        """ Getter for log level. """
        return self._level

    @property
    def name(self):
        """ Getter for logger name. """
        return self._name
