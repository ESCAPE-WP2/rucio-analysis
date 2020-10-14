import os
import random

from rucio_helper import create_did, list_rse_qos
from rucio_wrappers import RucioWrappersAPI, RucioWrappersCLI
from utility import generateRandomFile

from tests import Test


class TestReplicationQos(Test):
    """ Test replication for full grid of available QoS labels """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            # Assign variables from test.yml kwargs.
            #
            scope = kwargs["scope"]
            lifetime = kwargs["lifetime"]
            size = kwargs["size"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for test.")
            self.logger.critical(repr(e))
            exit()

        # Instantiate RucioWrappers to make Rucio calls
        #
        rucio_api = RucioWrappersAPI()
        rucio_cli = RucioWrappersCLI()

        # Create a dataset to house the data, named with today's date
        # and scope <scope>.
        #
        datasetDID = create_did(self.logger.name, scope)
        self.logger.debug("datasetDID: {}".format(datasetDID))

        # Obtain list of currently available QoS labels:
        #
        all_rses = [rse_dict["rse"] for rse_dict in rucio_api.list_rses()]
        all_qos = set(list_rse_qos(self.logger, all_rses).values())
        valid_qos = [qos for qos in all_qos if qos is not None]

        for qos_src in valid_qos:
            # Unable to upload with a QoS flag currently; therefore choose a source RSE
            # at random from RSEs with the <qos> label.
            try:
                source_rse = random.choice(
                    [
                        rse["rse"]
                        for rse in rucio_api.list_rses("QOS={}".format(qos_src))
                    ]
                )
            except Exception as e:
                self.logger.critical(repr(e))
                exit()

            f = generateRandomFile(size)
            fileDID = "{}:{}".format(scope, os.path.basename(f.name))

            # Upload to <rseSrc>
            self.logger.debug(
                "    Uploading file {} to RSE {}".format(f.name, source_rse)
            )
            try:
                rucio_cli.upload(
                    rse=source_rse, scope=scope, filePath=f.name, lifetime=lifetime
                )
                self.logger.debug("    Upload complete")
                os.remove(f.name)
            except Exception as e:
                self.logger.warning("Upload failed; ({})".format(repr(e)))
                os.remove(f.name)
                continue

            # Attach to dataset
            self.logger.debug("    Attaching file {} to {}".format(fileDID, datasetDID))
            try:
                rucio_cli.attach(todid=datasetDID, dids=fileDID)
                self.logger.debug("    Attached file to dataset")
            except Exception as e:
                self.logger.warning(repr(e))

            # Add replication rules for destination QoS
            self.logger.debug("    Adding QoS-based replication rules...")

            for i, qos_dest in enumerate(valid_qos):
                if qos_dest == qos_src:
                    continue
                self.logger.debug(
                    "    Replicate to destination with QOS {} with lifetime {} sec".format(
                        qos_dest, lifetime
                    )
                )
                try:
                    rtn = rucio_cli.addRule(
                        fileDID, 1, "QOS={}".format(qos_dest), lifetime=lifetime
                    )
                    self.logger.debug(
                        "      Rule ID: {}".format(
                            rtn.stdout.decode("UTF-8").rstrip("\n")
                        )
                    )
                except Exception as e:
                    self.logger.warning(repr(e))
                    continue

            self.logger.debug(
                "    Replication rules added for source QoS {}".format(qos_src)
            )

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
