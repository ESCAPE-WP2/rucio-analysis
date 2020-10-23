import os

from db import ES
from rucio_helper import createDID
from rucio_wrappers import RucioWrappersAPI
from tasks import Task
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
            protocols = kwargs['protocols']
            databases = kwargs['databases']
            taskName = kwargs['task_name']
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            exit()

        # Instantiate RucioWrappers class to allow access to static methods.
        #
        rucio = RucioWrappersAPI()

        # Create a dataset to house the data, named with today's date
        # and scope <scope>.
        #
        datasetDID = createDID(self.logger.name, scope)

        # Iteratively upload a file of size from <sizes> to each
        # RSE, attach to the dataset, add replication rules to the
        # other listed RSEs.
        #
        for rseSrc in rses:
            self.logger.info(
                bcolors.OKBLUE + "RSE (src): {}".format(rseSrc) + bcolors.ENDC
            )
            for protocol in protocols:
                for size in sizes:
                    self.logger.debug("File size: {} bytes".format(size))
                    for idx in range(nFiles):
                        # Generate random file of size <size>
                        f = generateRandomFile(size)
                        fileDID = "{}:{}".format(scope, os.path.basename(f.name))

                        # Upload to <rseSrc>
                        self.logger.debug(
                            "Uploading file {} of {} with protocol {}".format(
                                idx + 1, nFiles, protocol)
                        )
                        try:
                            rucio.upload(
                                rse=rseSrc, scope=scope, filePath=f.name, 
                                lifetime=lifetime, forceScheme=protocol
                            )
                        except Exception as e:
                            self.logger.warning(repr(e))
                            os.remove(f.name)
                            break
                        self.logger.debug("Upload complete")
                        os.remove(f.name)

                        # Attach to dataset
                        self.logger.debug(
                            "Attaching file {} to {}".format(fileDID, datasetDID)
                        )
                        try:
                            rucio.attach(todid=datasetDID, dids=fileDID)
                        except Exception as e:
                            self.logger.warning(repr(e))
                            break
                        self.logger.debug("Attached file to dataset")

                        # Push corresponding rules to database
                        for database in databases:
                            if database['type'] == 'es':
                                self.logger.debug("Injecting rules into ES database...")
                                es = ES(database['uri'], self.logger)
                                es.pushRulesForDID(fileDID, index=database['index'], 
                                    extraEntries={
                                        'task_name': taskName
                                    })

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))