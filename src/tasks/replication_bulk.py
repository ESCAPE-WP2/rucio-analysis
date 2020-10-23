from multiprocessing import Pool

from rucio_helper import createDID, uploadDirReplicate
from tasks import Task


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

        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            exit()

        loggerName = self.logger.name

        # Create a dataset to house the data, named with today's date
        # and scope <scope>.
        #
        datasetDID = createDID(loggerName, scope)

        self.logger.debug("Launching pool of {} workers".format(nWorkers))

        # Create array of args for each process
        #
        args_arr = [
            (
                loggerName,
                rseSrc,
                rsesDst,
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
        #
        with Pool(processes=nWorkers) as pool:
            pool.starmap(uploadDirReplicate, args_arr)
        pool.join()

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
