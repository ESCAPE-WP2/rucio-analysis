import os

from common.rucio.wrappers import RucioWrappersAPI
from tasks.task import Task
from utility import generateMetadataDict, generateRandomFile


class TestRucioMetadata(Task):
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
            scope = kwargs["scope"]
            lifetime = kwargs["lifetime"]
            size = kwargs["size"]
            fixed_metadata = kwargs["fixed_metadata"]
            metadata_props = kwargs["metadata_props"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for test.")
            self.logger.critical(repr(e))
            return False

        # Your code here.
        # START ---------------
        rucio = RucioWrappersAPI()
        self.logger.info(rucio.ping())

        # Generate random file of size <size> and upload.
        #
        f = generateRandomFile(size)
        did = "{}:{}".format(scope, os.path.basename(f.name))
        try:
            self.logger.info("Uploading file ({}) to {}...".format(did, upload_to))
            rucio.upload(
                rse=upload_to,
                scope=scope,
                filePath=f.name,
                lifetime=lifetime,
                logger=self.logger,
            )
            self.logger.debug("Upload complete")
        except Exception as e:
            self.logger.warning(repr(e))
        os.remove(f.name)

        # Build metadata dictionary
        meta_dict = generateMetadataDict(**metadata_props)
        if fixed_metadata:
            meta_dict = {**fixed_metadata, **meta_dict}

        # Set metadata and verify
        rucio.setMetadataBulk(did, meta_dict)
        retrieved_meta = rucio.getMetadata(did, plugin="JSON")
        if not retrieved_meta == meta_dict:
            # This crude verification assumes that fixed metadata is all set as JSON
            # (i.e. no DID Column match)
            self.logger.warning("Retrieved metadata does not match input")
        # END ---------------

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
