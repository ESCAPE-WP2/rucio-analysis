from tests import Test
from rucio_helper import list_rse_qos, create_did
from rucio_wrappers import RucioWrappersAPI, RucioWrappersCLI
from utility import generateRandomFile, bcolors
import os
import numpy as np


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
            qos = kwargs["qos"]
            scope = kwargs["scope"]
            total_lifecycle = kwargs["total_lifecycle"]
            size = 14500  # kB
            data_lifecycle = np.asarray(
                np.array([0.15, 1.0, 0.33, 0.66]) * total_lifecycle, dtype=int
            )
            pass
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for test.")
            self.logger.critical(repr(e))
            exit()

        # Your code here.
        self.logger.info(qos[0])
        # List of RSEs with the <qos_source> label
        source_rses = [
            rse["rse"] for rse in RucioWrappersAPI.list_rses("QOS={}".format(qos[0]))
        ]

        # TODO: cycle through the source_rses until successful upload achieved
        # (also, this could throw out of range error)
        source_rse = source_rses[1]
        # TODO: Other replication tests can use this helper
        datasetDID = create_did(self.logger, scope)

        rucio = RucioWrappersCLI()

        f = generateRandomFile(size)
        fileDID = "{}:{}".format(scope, os.path.basename(f.name))

        # Upload to <rseSrc>
        self.logger.debug("    Uploading file {} to RSE {}".format(f.name, source_rse))
        try:
            rucio.upload(
                rse=source_rse, scope=scope, filePath=f.name, lifetime=data_lifecycle[0]
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
            "    Replicate to destination with QOS {} with lifetime {} sec".format(
                qos[1], data_lifecycle[1]
            ),
            data_lifecycle[1],
        )
        self.addQoSRule(rucio, fileDID, qos[1], data_lifecycle[1])

        self.logger.debug(
            "    Replicate to destination with QOS {} with lifetime {} sec".format(
                qos[2], data_lifecycle[2]
            ),
            data_lifecycle[2],
        )
        self.addQoSRule(rucio, fileDID, qos[2], data_lifecycle[2])

        self.logger.debug(
            "    Replicate to destination with QOS {} with lifetime {} sec".format(
                qos[3], data_lifecycle[3]
            ),
            data_lifecycle[3],
        )
        self.addQoSRule(rucio, fileDID, qos[3], data_lifecycle[3])

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))

    def addQoSRule(self, rucio, fileDID, qos_dest, lifetime):
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
