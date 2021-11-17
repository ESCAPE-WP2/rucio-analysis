import logging
from multiprocessing import Pool
import os
import shutil
import time
import uuid

from common.es.rucio import Rucio as ESRucio
from common.rucio.helpers import createCollection
from common.rucio.wrappers import RucioWrappersCLI
from tasks.task import Task
from utility import bcolors, generateRandomFilesDir


def uploadDirReplicate(
    taskName,
    loggerName,
    databases,
    rseSrc,
    rsesDst,
    nFiles,
    fileSize,
    scope,
    lifetime,
    parentDID,
    dirIdx=1,
    nDirs=1,
    namingPrefix="",
):
    """
    Upload a dir containing <nFiles> of <fileSize> to <rseSrc>, attaching
    to <datasetDID> and adding replication rules for each of <rsesDst>.
    """
    logger = logging.getLogger(loggerName)
    logger.debug("Uploading directory {} of {}".format(dirIdx, nDirs))

    # Instantiate Rucio
    #
    rucio = RucioWrappersCLI()

    logger.info(bcolors.OKBLUE + "RSE (src): {}".format(rseSrc) + bcolors.ENDC)

    # Generate directory of files to be uploaded.
    #
    dirPath = generateRandomFilesDir(
        nFiles, fileSize, dirId=dirIdx, prefix=namingPrefix
    )

    # Create dataset DID based on directory name.
    #
    datasetName = os.path.basename(dirPath)
    datasetDID = createCollection(loggerName, scope, name=datasetName)

    # Attach directory dataset to parent container DID.
    #
    logger.debug("Attaching DID {} to {}".format(datasetDID, parentDID))
    try:
        rucio.attach(todid=parentDID, dids=datasetDID)
    except Exception as e:
        logger.warning(repr(e))
    logger.debug("Attached DID to collection.")

    # Upload to <rseSrc>
    #
    logger.debug("Uploading dir {} and attaching to {}".format(dirPath, parentDID))

    # Compose entry for database insertion.
    #
    if len(databases) > 0:
        entry = {
            "rule_id": str(uuid.uuid4()),
            "task_name": taskName,
            "file_size": fileSize,
            "n_files": nFiles,
            "type": "dataset",
            "to_rse": rseSrc,
            "scope": scope,
            "name": os.path.basename(dirPath),
            "is_upload_submitted": 1,
        }
        try:
            st = time.time()
            rucio.uploadDir(rseSrc, scope, dirPath, lifetime, datasetDID)
            entry["upload_duration"] = time.time() - st
            entry["state"] = "UPLOAD-SUCCESSFUL"
        except Exception as e:
            logger.warning("Upload failed: {}".format(e))
            entry["error"] = repr(e.__class__.__name__).strip("'")
            entry["state"] = "UPLOAD-FAILED"
            shutil.rmtree(dirPath)

        try:
            rtn = rucio.addRule(datasetDID, 1, rseSrc, lifetime=lifetime)
            logger.debug(
                "Added Rule ID: {} for DID {} and source RSE {}".format(
                    rtn.stdout.decode("UTF-8").rstrip("\n"), datasetDID, rseSrc
                ),
            )
        except Exception as e:
            logger.warning(repr(e))

        # Push corresponding upload rules to database.
        if databases is not None:
            for database in databases:
                if database["type"] == "es":
                    logger.debug("Injecting upload rules into ES database...")
                    es = ESRucio(database["uri"], logger)
                    es.pushRulesForDID(
                        datasetDID, index=database["index"], baseEntry=entry)

    if not os.path.exists(dirPath):  # upload failed
        return
    logger.debug("Upload complete")

    # Add replication rules for other RSEs.
    #
    if rsesDst is not None:
        for rseDst in rsesDst:
            if rseSrc == rseDst:
                continue
            try:
                rtn = rucio.addRule(datasetDID, 1, rseDst, src=rseSrc, lifetime=lifetime)
                logger.debug(
                    "Added Rule ID: {} for DID {} and RSE {}".format(
                        rtn.stdout.decode("UTF-8").rstrip("\n"), datasetDID, rseDst
                    ),
                )
            except Exception as e:
                logger.warning(repr(e))
                continue

            # Push corresponding replication rules to database.
            if databases is not None:
                for database in databases:
                    if database["type"] == "es":
                        logger.debug("Injecting replication rules into ES database... ")
                        es = ESRucio(database["uri"], logger)
                        es.pushRulesForDID(
                            datasetDID,
                            index=database["index"],
                            baseEntry={
                                "task_name": taskName,
                                "file_size": fileSize,
                                "n_files": nFiles,
                                "type": "dataset",
                                "is_submitted": 1,
                            },
                        )
        logger.debug("All replication rules added")
    else:
        logger.debug("No destination RSEs passed; no replication rules added")
    shutil.rmtree(dirPath)


class TestReplicationBulk(Task):
    """
    Rucio upload directories of files in parallel to a source RSE and
    replicate on a destination RSE.
    """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            nWorkers = kwargs["n_workers"]
            nDirs = kwargs["n_dirs"]
            nFiles = kwargs["n_files"]
            fileSize = kwargs["file_size"]
            lifetime = kwargs["lifetime"]
            rseSrc = kwargs["source_rse"]
            rsesDst = kwargs["dest_rses"]
            scope = kwargs["scope"]
            containerName = kwargs.get("container_name", None)
            namingPrefix = kwargs.get("naming_prefix", "")
            databases = kwargs["databases"]
            taskName = kwargs["task_name"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        loggerName = self.logger.name

        # Create a DID to group the data, by default named with today's date
        # and scope <scope>.
        #
        if containerName is None:
            parentDID = createCollection(loggerName, scope, collectionType="CONTAINER")
        else:
            parentDID = createCollection(
                loggerName, scope, name=containerName, collectionType="CONTAINER")

        self.logger.debug("Launching pool of {} workers".format(nWorkers))

        # Create array of args for each process
        #
        args_arr = [
            (
                taskName,
                loggerName,
                databases,
                rseSrc,
                rsesDst,
                nFiles,
                fileSize,
                scope,
                lifetime,
                parentDID,
                dirIdx,
                nDirs,
                namingPrefix,
            )
            for dirIdx in range(1, nDirs + 1)
        ]

        # Launch pool of worker processes, and join() to wait for all to complete
        #
        with Pool(processes=nWorkers) as pool:
            pool.starmap(uploadDirReplicate, args_arr)
        pool.join()

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
