import logging
import sys


class Logger:
    def __init__(
        self,
        name="root",
        level="DEBUG",
        fmt="%(asctime)s [%(name)s] %(levelname).4s\t%(process)d\t%(message)s",
        fmt_prefix='',
        fmt_suffix='',
        addConsoleHandler=True
    ):
        self._fmt = fmt
        self._name = name
        self._level = level
        self.formatter = logging.Formatter(self.fmt)

        # logger must capture all logging (DEBUG), handlers can restrict this.
        self.get().setLevel("DEBUG")

        self._clearHandlers()
        if addConsoleHandler:
            self.addConsoleHandler()

    def _clearHandlers(self):
        self.get().handlers = []

    def addConsoleHandler(self):
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(self.level)
        ch.setFormatter(fmt=self.formatter)
        self.get().addHandler(ch)

    @property
    def config(self):
        return self.get().config

    @property
    def fmt(self):
        return self._fmt

    def get(self):
        return logging.getLogger(self.name)

    @property
    def level(self):
        return self._level

    @property
    def name(self):
        return self._name
