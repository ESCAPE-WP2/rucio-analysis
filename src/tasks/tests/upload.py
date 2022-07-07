from datetime import datetime
import os
import time

from common.es.rucio import Rucio as ESRucio
from common.rucio.helpers import createCollection
from common.rucio.wrappers import RucioWrappersAPI
from tasks.task import Task
from utility import bcolors, generateRandomFile


class TestUpload(Task):
    """ Rucio file upload to a list of RSEs. """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            nFiles = kwargs["n_files"]
            rses = kwargs["rses"]
            scope = kwargs["scope"]
            lifetime = kwargs["lifetime"]
            sizes = kwargs["sizes"]
            protocols = kwargs["protocols"]
            databases = kwargs["databases"]
            taskName = kwargs["task_name"]
            namingPrefix = kwargs.get("naming_prefix", "")
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        # Instantiate RucioWrappers class to allow access to static methods.
        #
        rucio = RucioWrappersAPI()

        # Create a dataset to house the data, named with today's date
        # and scope <scope>.
        #
        datasetDID = createCollection(self.logger.name, scope)

        # Iteratively upload a file of size from <sizes> to each
        # RSE, attach to the dataset, add replication rules to the
        # other listed RSEs.
        #
        for rseDst in rses:
            self.logger.info(
                bcolors.OKBLUE + "RSE (dst): {}".format(rseDst) + bcolors.ENDC
            )
            for protocol in protocols:
                for size in sizes:
                    self.logger.debug("File size: {} bytes".format(size))
                    for idx in range(nFiles):
                        # Generate random file of size <size>
                        f = generateRandomFile(size, prefix=namingPrefix)
                        fileDID = "{}:{}".format(scope, os.path.basename(f.name))

                        # Upload to <rseDst>
                        self.logger.debug(
                            "Uploading file {} of {} with protocol {}".format(
                                idx + 1, nFiles, protocol
                            )
                        )

                        entry = {
                            "task_name": taskName,
                            "file_size": size,
                            "type": "file",
                            "n_files": 1,
                            "is_upload_submitted": 1,
                        }
                        try:
                            st = time.time()
                            rucio.upload(
                                logger=self.logger,
                                rse=rseDst,
                                scope=scope,
                                filePath=f.name,
                                lifetime=lifetime,
                                forceScheme=protocol,
                            )
                            entry["upload_duration"] = time.time() - st
                            entry["state"] = "UPLOAD-SUCCESSFUL"
                            self.logger.debug("Upload complete")

                            # Attach to dataset
                            self.logger.debug(
                                "Attaching file {} to {}".format(fileDID, datasetDID)
                            )
                            try:
                                rucio.attach(todid=datasetDID, dids=fileDID)
                            except Exception as e:
                                self.logger.warning(repr(e))
                            self.logger.debug("Attached file to dataset")
                        except Exception as e:
                            self.logger.warning("Upload failed: {}".format(e))
                            now = datetime.now()
                            entry["created_at"] = now.isoformat() 
                            entry["scope"] = scope
                            entry["name"] = os.path.basename(f.name)
                            entry["from_rse"] = None
                            entry["to_rse"] = rseDst
                            entry["error"] = repr(e.__class__.__name__).strip("'")
                            entry["error_details"] = repr(e).strip("'")
                            entry["protocol"] = protocol
                            entry["state"] = "UPLOAD-FAILED"
                        os.remove(f.name)

                        # Push corresponding rules to database
                        if databases is not None:
                            for database in databases:
                                if database["type"] == "es":
                                    self.logger.debug(
                                        "Injecting rules into ES database...")
                                    es = ESRucio(database["uri"], self.logger)
                                    es.pushRulesForDID(
                                        fileDID, index=database["index"],
                                        baseEntry=entry
                                    )

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
