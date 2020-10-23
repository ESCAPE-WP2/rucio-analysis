import logging
import os
import shutil
from datetime import datetime

from rucio_wrappers import RucioWrappersCLI
from utility import bcolors, generateDirRandomFiles


def uploadDirReplicate(
    loggerName,
    rseSrc,
    rsesDst,
    nFiles,
    fileSize,
    scope,
    lifetime,
    datasetDID,
    dirIdx=1,
    nDirs=1,
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
    dirPath = generateDirRandomFiles(nFiles, fileSize, dirIdx)

    # Upload to <rseSrc>
    logger.debug("Uploading dir {} and attaching to {}".format(dirPath, datasetDID))

    try:
        rucio.uploadDir(rseSrc, scope, dirPath, lifetime, datasetDID)
    except Exception as e:
        logger.warning(repr(e))
        shutil.rmtree(dirPath)
        return
    logger.debug("Upload complete")

    # Add replication rules for other RSEs
    for rseDst in rsesDst:
        if rseSrc == rseDst:
            continue
        for filename in os.listdir(dirPath):
            fileDID = "{}:{}".format(scope, filename)
            logger.debug("Adding replication rule for {}".format(fileDID))
            logger.debug(
                bcolors.OKGREEN + "RSE (dst): {}".format(rseDst) + bcolors.ENDC
            )
            try:
                rtn = rucio.addRule(fileDID, 1, rseDst, lifetime=lifetime)
                logger.debug(
                    "Rule ID: {}".format(rtn.stdout.decode("UTF-8").rstrip("\n"))
                )
            except Exception as e:
                logger.warning(repr(e))
                continue
    logger.debug("All replication rules added")
    shutil.rmtree(dirPath)


def createDID(
    loggerName,
    scope,
):
    """ Create a new DID in the passed scope """

    logger = logging.getLogger(loggerName)

    # Instantiate RucioWrappers class to access to static methods.
    #
    rucio = RucioWrappersCLI()

    # Create a dataset to house the data, named with today's date
    # and scope <scope>.
    #
    # Does not try to create a dataset if it already exists.
    #
    todaysDate = datetime.now().strftime("%d-%m-%Y")
    datasetDID = "{}:{}".format(scope, todaysDate)
    logger.info("Checking for dataset ({})".format(datasetDID))
    try:
        dids = rucio.listDIDs(scope=scope)
    except Exception as e:
        logger.critical("Error listing dataset")
        logger.critical(repr(e))
        exit()
    if datasetDID not in dids:
        logger.debug("Adding dataset")
        try:
            rucio.addDataset(did=datasetDID)
        except Exception as e:
            logger.critical("Error adding dataset")
            logger.critical(repr(e))
            exit()
    else:
        logger.debug("Dataset already exists")

    return datasetDID
