import os

from common.rucio.wrappers import RucioWrappersAPI
from tasks.task import Task
from utility import generateRandomFile


class StubHelloWorld(Task):
    """ Hello World test class stub. """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            # Assign variables from tests.stubs.yml kwargs.
            #
            text = kwargs['text']
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        # Your code here.
        # START ---------------
        self.logger.info(text)
        # END ---------------

        self.toc()
        self.logger.info("Finished in {}s".format(
            round(self.elapsed)))


class StubRucioAPI(Task):
    """ Rucio API test class stub. """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            pass
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for test.")
            self.logger.critical(repr(e))
            return False

        # Your code here.
        # START ---------------
        rucio = RucioWrappersAPI()
        self.logger.info(rucio.ping())
        self.logger.info(rucio.whoAmI())

        # END ---------------

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
