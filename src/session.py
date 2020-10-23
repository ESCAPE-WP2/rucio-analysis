import yaml


class Session():
    def __init__(self, tasks, logger):
        self.logger = logger
        self._tasks = None

        self._parseTasksFile(tasks)

    def _parseTasksFile(self, path):
        """ Parse a configuration yaml file. """
        self.logger.info("Parsing tasks file")
        try:
            with open(path) as f:
                contents = f.read()
                try:
                    self._tasks = yaml.safe_load(contents)
                except yaml.scanner.ScannerError as e:
                    self.logger.critical("Could not parse yaml.")
                    self.logger.critical(repr(e))
                    exit()
        except IOError as e:
            self.logger.critical("Configuration file not found.")
            self.logger.critical(repr(e))
            exit()

    @property
    def tasks(self):
        return self._tasks
