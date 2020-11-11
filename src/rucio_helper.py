import logging
import os
import shutil
from datetime import datetime

from rucio_wrappers import RucioWrappersCLI, RucioWrappersAPI
from utility import bcolors, generateDirRandomFiles


def uploadDirReplicate(
    loggerName,
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

    try:
        rucio.uploadDir(rseSrc, scope, dirPath, lifetime, datasetDID)
    except Exception as e:
        logger.warning(repr(e))
        shutil.rmtree(dirPath)
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
        todaysDate = datetime.now().strftime("%d-%m-%Y")
        DID = "{}:{}".format(scope, todaysDate)
    logger.info("Checking for DID ({})".format(DID))

    try:
        dids = rucio.listDIDs(scope=scope)
    except Exception as e:
        logger.critical("Error listing Collection")
        logger.critical(repr(e))
        exit()

    if DID not in dids:
        logger.debug("Adding DID {} of type {}".format(DID, collectionType))
        try:
            rucio.addDID(DID, collectionType)
        except Exception as e:
            logger.critical("Error adding collection")
            logger.critical(repr(e))
            exit()
    else:
        logger.debug("Collection already exists")

    return DID
