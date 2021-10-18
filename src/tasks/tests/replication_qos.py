import os
import random

from common.es.rucio import Rucio as ESRucio
from common.rucio.helpers import createCollection
from common.rucio.wrappers import RucioWrappersAPI, RucioWrappersCLI
from tasks.task import Task
from utility import generateRandomFile


class TestReplicationQos(Task):
    """ Test replication for full grid of available QoS labels. """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            qos = kwargs["qos"]
            scope = kwargs["scope"]
            lifetimes = kwargs["lifetimes"]
            size = kwargs["size"]
            databases = kwargs["databases"]
            taskName = kwargs["task_name"]
            namingPrefix = kwargs.get("naming_prefix", "")
            if len(qos) != len(lifetimes):
                self.logger.critical(
                    "{} qos and {} lifetimes passed. "
                    "Expected the same number of each arg.".format(
                        len(qos), len(lifetimes)
                    )
                )
                return False
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        # Instantiate RucioWrappers to make Rucio calls
        #
        rucio_api = RucioWrappersAPI()
        rucio_cli = RucioWrappersCLI()

        # Create a dataset to house the data, named with today's date
        # and scope <scope>.
        #
        datasetDID = createCollection(self.logger.name, scope)
        self.logger.debug("datasetDID: {}".format(datasetDID))

        # Unable to upload with a QoS flag currently; therefore choose a source RSE
        # at random from RSEs with the <qos> label.
        #
        qos_src = qos.pop(0)
        life_src = lifetimes.pop(0)
        try:
            source_rse = random.choice(
                [rse["rse"] for rse in rucio_api.listRSEs("QOS={}".format(qos_src))]
            )
        except Exception as e:
            self.logger.critical(repr(e))
            return False

        f = generateRandomFile(size, prefix=namingPrefix)
        fileDID = "{}:{}".format(scope, os.path.basename(f.name))

        # Upload to <rseSrc>
        #
        #
        self.logger.debug(
            "Uploading file {} to RSE {} (QoS: {})".format(f.name, source_rse, qos_src)
        )
        try:
            rucio_cli.upload(
                rse=source_rse, scope=scope, filePath=f.name, lifetime=life_src
            )
            self.logger.debug("Upload complete")
            os.remove(f.name)
        except Exception as e:
            self.logger.warning("Upload failed; ({})".format(repr(e)))
            os.remove(f.name)
            return False

        # Attach to dataset
        #
        self.logger.debug("Attaching file {} to {}".format(fileDID, datasetDID))
        try:
            rucio_cli.attach(todid=datasetDID, dids=fileDID)
            self.logger.debug("Attached file to dataset")
        except Exception as e:
            self.logger.warning(repr(e))

        # Add replication rules for destination QoS
        #
        self.logger.debug("Adding QoS-based replication rules...")

        for i, qos_dest in enumerate(qos):
            self.logger.debug(
                "Replicate to destination with QOS {} with lifetime {} sec".format(
                    qos_dest, lifetimes[i]
                )
            )
            try:
                rtn = rucio_cli.addRule(
                    fileDID,
                    1,
                    "QOS={}".format(qos_dest),
                    lifetime=lifetimes[i],
                    activity="Debug",
                    src="QOS={}".format(qos_src),
                )
                self.logger.debug(
                    "Rule ID: {}".format(rtn.stdout.decode("UTF-8").rstrip("\n"))
                )
            except Exception as e:
                self.logger.warning(repr(e))
                continue

        # Push corresponding rules to database
        #
        if databases is not None:
            for database in databases:
                if database["type"] == "es":
                    self.logger.debug("Injecting rules into ES database...")
                    es = ESRucio(database["uri"], self.logger)
                    es.pushRulesForDID(
                        fileDID,
                        index=database["index"],
                        baseEntry={
                            "task_name": taskName,
                            "file_size": size,
                            "type": "file",
                            "n_files": 1,
                            "is_submitted": 1,
                        },
                    )

        self.logger.debug("Replication rules added for source QoS {}".format(qos_src))

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
