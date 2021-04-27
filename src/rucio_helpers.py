from datetime import datetime
import logging
import os
import shutil
import time
import uuid

from es import ESRucio
from rucio_wrappers import RucioWrappersCLI, RucioWrappersAPI
from utility import bcolors, generateRandomFilesDir


def createCollection(loggerName, scope, name=None, collectionType="DATASET"):
    """ Create a new collection in scope, <scope>. """

    logger = logging.getLogger(loggerName)

    rucio = RucioWrappersAPI()

    # If name is not specified, create one according to datestamp.
    #
    if name is None:
        name = datetime.now().strftime("%d-%m-%Y")
        if collectionType == "CONTAINER":
            name = "container_{}".format(name)

    # Create container DID according to <scope>:<name> format.
    #
    did = "{}:{}".format(scope, name)

    logger.info("Checking to see if DID ({}) already exists...".format(did))
    try:
        # Check to see if DID already exists, and if not, add.
        dids = rucio.listDIDs(scope=scope)
        if did not in dids:
            logger.debug("Adding DID {} of type {}".format(did, collectionType))
            try:
                rucio.addDID(did, collectionType)
            except Exception as e:
                logger.critical("Error adding did.")
                logger.critical(repr(e))
                return False
        else:
            logger.debug("DID already exists. Skipping.")

    except Exception as e:
        logger.critical("Error listing collection.")
        logger.critical(repr(e))
        return False

    return did


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

        # Push corresponding upload rules to database.
        for database in databases:
            if database["type"] == "es":
                logger.debug("Injecting upload rules into ES database...")
                es = ESRucio(database["uri"], logger)
                es.pushRulesForDID(datasetDID, index=database["index"], baseEntry=entry)

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
                rtn = rucio.addRule(datasetDID, 1, rseDst, lifetime=lifetime)
                logger.debug(
                    "Added Rule ID: {} for DID {} and RSE {}".format(
                        rtn.stdout.decode("UTF-8").rstrip("\n"), datasetDID, rseDst
                    ),
                )
            except Exception as e:
                logger.warning(repr(e))
                continue

            # Push corresponding replication rules to database.
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


def createPFN(loggerName, rse, scope, testdir, project="", dataset="", filename=""):
    logger = logging.getLogger(loggerName)

    rucio = RucioWrappersAPI()
    protocols = rucio.getRSEProtocols(rse)

    rse_prefix = (
        protocols[0]["scheme"]
        + "://"
        + protocols[0]["hostname"]
        + protocols[0]["prefix"]
    )
    logger.info("RSE prefix constructed is {}".format(rse_prefix))
    pfn = os.path.join(rse_prefix, scope, testdir, project, dataset, filename)
    return pfn
