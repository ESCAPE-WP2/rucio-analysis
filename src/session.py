import yaml

class Session():
    def __init__(self, tests, logger):
        self.logger = logger
        self._tests = None

        self._parseTestsFile(tests)


    def _parseTestsFile(self, path):
        """ Parse a tests yaml file. """
        self.logger.info("Parsing tests file")
        try:
            with open(path) as f:
                contents = f.read()
                try:
                    self._tests = yaml.safe_load(contents)
                except yaml.scanner.ScannerError as e:
                    self.logger.critical("Could not parse yaml.")
                    self.logger.critical(repr(e))
                    exit()                    
        except IOError as e:
            self.logger.critical("Configuration file not found.")
            self.logger.critical(repr(e))
            exit()


    @property
    def tests(self):
        return self._tests

