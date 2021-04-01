import os

from rucio_wrappers import RucioWrappersAPI
from tasks import Task
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
            # Assign variables from test.yml kwargs.
            #
            upload_to = kwargs["upload_to"]
            replicate_to = kwargs["replicate_to"]
            scope = kwargs["scope"]
            lifetime = kwargs["lifetime"]
            size = kwargs["size"]
            download = kwargs["download"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for test.")
            self.logger.critical(repr(e))
            return False

        # Your code here.
        # START ---------------
        rucio = RucioWrappersAPI()
        self.logger.info(rucio.whoAmI())

        # Generate random file of size <size> and upload.
        #
        f = generateRandomFile(size)
        did = "{}:{}".format(scope, os.path.basename(f.name))
        try:
            self.logger.debug("Uploading file ({}) to {}...".format(did, upload_to))
            rucio.upload(rse=upload_to, scope=scope, filePath=f.name,
                         lifetime=lifetime, logger=self.logger)
            self.logger.debug("Upload complete")
            if replicate_to is not None:
                self.logger.debug("Replicating file ({}) to {}...".format(did,
                                                                          replicate_to))
                rucio.addRule(did, 1, replicate_to, lifetime=lifetime,
                              src=upload_to)
                self.logger.debug("Replication complete")
            if download:
                self.logger.debug("Downloading file ({})...".format(did))
                rucio.download(did)
                self.logger.debug("Download complete")
        except Exception as e:
            self.logger.warning(repr(e))
        os.remove(f.name)
        # END ---------------

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
