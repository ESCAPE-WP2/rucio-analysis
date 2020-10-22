import datetime
import os

from elasticsearch import Elasticsearch

from rucio_wrappers import RucioWrappersAPI
from tests import Test
from utility import generateRandomFile


class TestStubHelloWorld(Test):
    """ Hello World test class stub.
    """
    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            # Assign variables from test.yml kwargs.
            #
            text = kwargs['text']
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for test.")
            self.logger.critical(repr(e))
            exit()

        # Your code here.
        # ---------------
        self.logger.info(text)
        # ---------------

        self.toc()
        self.logger.info("Finished in {}s".format(
            round(self.elapsed)))


class TestStubRucioAPI(Test):
    """ Rucio API test class stub.
    """
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
            exit()

        # Your code here.
        # ---------------
        rucio = RucioWrappersAPI()
        self.logger.info(rucio.whoami())

        # Generate random file of size <size> and upload.
        #
        f = generateRandomFile(size)
        did = "{}:{}".format(scope, os.path.basename(f.name))
        try:
            self.logger.debug("Uploading file ({}) to {}...".format(did, upload_to))
            rucio.upload(rse=upload_to, scope=scope, filePath=f.name, lifetime=lifetime)
            self.logger.debug("  Upload complete")
            if replicate_to is not None:
                self.logger.debug("Replicating file ({}) to {}...".format(did,
                    replicate_to))
                rucio.addRule(did, 1, replicate_to, lifetime=lifetime,
                    src_rse=upload_to)
                self.logger.debug("  Replication complete")
            if download:
                self.logger.debug("Downloading file ({})...".format(did))
                rucio.download(did)
                self.logger.debug("  Download complete")
        except Exception as e:
            self.logger.warning(repr(e))
        os.remove(f.name)
        # ---------------

        self.toc()
        self.logger.info("Finished in {}s".format(
            round(self.elapsed)))