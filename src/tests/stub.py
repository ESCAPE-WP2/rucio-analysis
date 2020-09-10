from tests import Test

class TestStub(Test):
    """ Stub test class.
    """
    def __init__(self, logger):
        super().__init__(logger)


    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            # Assign variables from test.yml kwargs.
            #
            pass
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for test.")
            self.logger.critical(repr(e))
            exit()

        self.toc()
        self.logger.info("Finished in {}s".format(
            round(self.elapsed)))