import logging
import os
import shutil
from datetime import datetime
from multiprocessing import Pool

from rucio import Rucio
from utility import bcolors, generateDirRandomFiles

from tests import Test


class TestReplicationBulk(Test):
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
            # Assign variables from test.yml kwargs.
            #
            nWorkers = kwargs["n_workers"]
            nDirs = kwargs["n_dirs"]
            nFiles = kwargs["n_files"]
            fileSize = kwargs["file_size"]
            lifetime = kwargs["lifetime"]
            rseSrc = kwargs["source_rse"]
            rseDest = kwargs["dest_rse"]
            scope = kwargs["scope"]

        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for test.")
            self.logger.critical(repr(e))
            exit()

        # Instantiate Rucio class to allow access to static methods.
        #
        rucio = Rucio()

        # Create a dataset to house the data, named with today's date
        # and scope <scope>.
        #
        # Does not try to create a dataset if it already exists.
        #
        todaysDate = datetime.now().strftime("%d-%m-%Y")
        datasetDID = "{}:{}".format(scope, todaysDate)
        self.logger.info("Checking for dataset ({})".format(datasetDID))
        try:
            dids = rucio.listDIDs(scope=scope)
        except Exception as e:
            self.logger.critical("Error listing dataset")
            self.logger.critical(repr(e))
            exit()
        if datasetDID not in dids:
            self.logger.debug("Adding dataset")
            try:
                rucio.addDataset(did=datasetDID)
            except Exception as e:
                self.logger.critical("Error adding dataset")
                self.logger.critical(repr(e))
                exit()
        else:
            self.logger.debug("Dataset already exists")

        self.logger.debug("Launching pool of {} workers".format(nWorkers))

        loggerName = self.logger.name

        # Create array of args for each process
        #
        args_arr = [
            (
                loggerName,
                rseSrc,
                rseDest,
                nFiles,
                fileSize,
                scope,
                lifetime,
                datasetDID,
                dirIdx,
                nDirs,
            )
            for dirIdx in range(1, nDirs + 1)
        ]

        # Launch pool of worker processes, and join() to wait for all to complete
        with Pool(processes=nWorkers) as pool:
            pool.starmap(upload_dir, args_arr)
        pool.join()

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))


def upload_dir(
    loggerName,
    rseSrc,
    rseDst,
    nFiles,
    fileSize,
    scope,
    lifetime,
    datasetDID,
    dirIdx=1,
    nDirs=1,
):
    """
    Upload a dir containing <nFiles> of <fileSize> to <rseSrc>,
    attaching to <datasetDID>, add replication rules for <rseDst>.
    """
    logger = logging.getLogger(loggerName)
    logger.debug("    Uploading directory {} of {}".format(dirIdx, nDirs))

    # Instantiate Rucio
    rucio = Rucio()

    logger.info(bcolors.OKBLUE + "RSE (src): {}".format(rseSrc) + bcolors.ENDC)

    # Generate directory:
    dirPath = generateDirRandomFiles(nFiles, fileSize, dirIdx)

    # Upload to <rseSrc>
    logger.debug("    Uploading dir {} and attaching to {}".format(dirPath, datasetDID))

    try:
        rucio.upload_dir(rseSrc, scope, dirPath, lifetime, datasetDID)
    except Exception as e:
        logger.warning(repr(e))
        shutil.rmtree(dirPath)
        return
    logger.debug("    Upload complete")

    # Add replication rules for other RSEs
    for filename in os.listdir(dirPath):
        fileDID = "{}:{}".format(scope, filename)
        logger.debug("    Adding replication rule for {}".format(fileDID))
        logger.debug(
            bcolors.OKGREEN + "    RSE (dst): {}".format(rseDst) + bcolors.ENDC
        )
        try:
            rtn = rucio.addRule(fileDID, 1, rseDst, lifetime=lifetime)
            logger.debug(
                "      Rule ID: {}".format(rtn.stdout.decode("UTF-8").rstrip("\n"))
            )
        except Exception as e:
            logger.warning(repr(e))
            continue
    logger.debug("    All replication rules added")
    shutil.rmtree(dirPath)
