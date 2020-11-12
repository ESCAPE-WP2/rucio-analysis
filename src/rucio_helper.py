from datetime import datetime
import logging
import os
import shutil
import time
import uuid

from db import ES
from rucio_wrappers import RucioWrappersCLI, RucioWrappersAPI
from utility import bcolors, generateDirRandomFiles


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
    data_tag="",
):
    """
    Upload a dir containing <nFiles> of <fileSize> to <rseSrc>, attaching
    to <datasetDID>, add replication rules for each of <rsesDst>.
    """
    logger = logging.getLogger(loggerName)
    logger.debug("Uploading directory {} of {}".format(dirIdx, nDirs))

    # Instantiate Rucio
    rucio = RucioWrappersCLI()

    logger.info(bcolors.OKBLUE + "RSE (src): {}".format(rseSrc) + bcolors.ENDC)

    # Generate directory:
    dirPath = generateDirRandomFiles(nFiles, fileSize, dirId=dirIdx, prefix=data_tag)

    # Create dataset DID based on directory name
    datasetName = os.path.basename(dirPath)
    datasetDID = createDID(
        loggerName, scope, "DATASET", "{}:{}".format(scope, datasetName)
    )

    # Attach dir dataset to container DID
    logger.debug("Attaching DID {} to {}".format(datasetDID, parentDID))
    try:
        rucio.attach(todid=parentDID, dids=datasetDID)
    except Exception as e:
        logger.warning(repr(e))
    logger.debug("Attached DID to collection")

    # Upload to <rseSrc>
    logger.debug("Uploading dir {} and attaching to {}".format(dirPath, parentDID))

    entry = {
        "rule_id": str(uuid.uuid4()),
        "task_name": taskName,
        "file_size": fileSize,
        "n_files": nFiles,
        "type": "dataset",
        "to_rse": rseSrc,
        "scope": scope,
        "name": os.path.basename(dirPath),
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

    # Push corresponding upload rules to database
    for database in databases:
        if database["type"] == "es":
            logger.debug("Injecting upload rules into ES database...")
            es = ES(database["uri"], logger)
            es.pushRulesForDID(datasetDID, index=database["index"], baseEntry=entry)

    if not os.path.exists(dirPath):  # upload failed
        return

    logger.debug("Upload complete")

    # Add replication rules for other RSEs
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

            # Push corresponding replication rules to database
            for database in databases:
                if database["type"] == "es":
                    logger.debug("Injecting replication rules into ES database... ")
                    es = ES(database["uri"], logger)
                    es.pushRulesForDID(
                        datasetDID,
                        index=database["index"],
                        baseEntry={
                            "task_name": taskName,
                            "file_size": fileSize,
                            "n_files": nFiles,
                            "type": "dataset",
                        },
                    )
        logger.debug("All replication rules added")
    else:
        logger.debug("No destination RSEs passed; no replication rules added")
    shutil.rmtree(dirPath)


def createDID(loggerName, scope, collectionType="DATASET", DID=None):
    """ Create a new DID in the passed scope """

    logger = logging.getLogger(loggerName)

    # Instantiate RucioWrappers class to access to static methods.
    #
    rucio = RucioWrappersAPI()
    # Create a collection to house the data,
    # Default is dataset named with today's date
    # and scope <scope>.
    #
    # Does not try to create a collection if it already exists.
    #
    if DID is None:
        DIDName = datetime.now().strftime("%d-%m-%Y")
        if collectionType == "CONTAINER":
            DIDName = "container_{}".format(DIDName)
        DID = "{}:{}".format(scope, DIDName)
    logger.info("Checking for DID ({})".format(DID))

    try:
        dids = rucio.listDIDs(scope=scope)
    except Exception as e:
        logger.critical("Error listing Collection")
        logger.critical(repr(e))
        return False

    if DID not in dids:
        logger.debug("Adding DID {} of type {}".format(DID, collectionType))
        try:
            rucio.addDID(DID, collectionType)
        except Exception as e:
            logger.critical("Error adding collection")
            logger.critical(repr(e))
            return False
    else:
        logger.debug("Collection already exists")

    return DID
