from tests import Test
from rucio_helper import list_rse_qos, create_did
from rucio_wrappers import RucioWrappersAPI, RucioWrappersCLI
from utility import generateRandomFile, bcolors
import os
import time


class TestReplicationQos(Test):
    """Test replication by QoS"""

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            # Assign variables from test.yml kwargs.
            #
            qos_source = kwargs["qos_source"]
            qos_dest = kwargs["qos_dest"]
            scope = kwargs["scope"]
            size = 14000  # kB
            lifetime = 3600  # s

            # rses = kwargs["rses"]
            pass
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for test.")
            self.logger.critical(repr(e))
            exit()

        # Your code here.
        self.logger.info(qos_source)

        source_rses = [
            rse["rse"]
            for rse in RucioWrappersAPI.list_rses("QOS={}".format(qos_source))
        ]

        source_rse = source_rses[0]  # TODO: Pick one at random

        # TODO: Other replication tests can use this helper
        datasetDID = create_did(self.logger, scope)

        rucio = RucioWrappersCLI()

        f = generateRandomFile(size)
        fileDID = "{}:{}".format(scope, os.path.basename(f.name))

        # Upload to <rseSrc>
        self.logger.debug("    Uploading file {}".format(f.name))
        try:
            rucio.upload(
                rse=source_rse, scope=scope, filePath=f.name, lifetime=lifetime
            )
            self.logger.debug("    Upload complete")
            os.remove(f.name)
        except Exception as e:
            self.logger.warning(repr(e))
            os.remove(f.name)

        # Attach to dataset
        self.logger.debug("    Attaching file {} to {}".format(fileDID, datasetDID))
        try:
            rucio.attach(todid=datasetDID, dids=fileDID)
            self.logger.debug("    Attached file to dataset")
        except Exception as e:
            self.logger.warning(repr(e))

        # Add replication rules for destination QoS
        self.logger.debug("    Adding replication rules...")
        self.logger.debug(
            bcolors.OKGREEN + "    RSE (QoS dst): {}".format(qos_dest) + bcolors.ENDC
        )
        try:
            # TODO: Either add separate method or specify in addRule method that it
            # can take either RSE or RSE expression
            rtn = rucio.addRule(
                fileDID, 1, "QOS={}".format(qos_dest), lifetime=lifetime
            )
            self.logger.debug(
                "      Rule ID: {}".format(rtn.stdout.decode("UTF-8").rstrip("\n"))
            )
            self.logger.debug("    Replication rules added")
        except Exception as e:
            self.logger.warning(repr(e))

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
