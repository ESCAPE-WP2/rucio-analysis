from datetime import datetime
import os

from rucio import Rucio
from tests import Test
from utility import generateRandomFile

class TestReplication(Test):
    """ Rucio file upload/replication to a list of RSEs.
    """
    def __init__(self, logger):
        super().__init__(logger)


    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            # Assign variables from test.yml kwargs.
            #
            copies = kwargs['copies']
            rses = kwargs['rses']
            scope = kwargs['scope']
            sizes = kwargs['sizes']
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
        todaysDate = datetime.now().strftime('%m-%d-%Y')
        datasetDID = '{}:{}'.format(scope, todaysDate)
        self.logger.info("Checking for dataset ({})".format(
            datasetDID))
        try:
            dids = rucio.listDIDs(scope='rucio-testing')
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

        # Recursively upload a file of size from <sizes> to the first 
        # RSE, attach to the dataset, add add replication rules to the 
        # other listed RSEs.
        #
        self.logger.info("Starting upload...")
        self.logger.info("RSE: {}".format(rses[0]))
        for size in sizes:
            self.logger.debug("  File size: {} bytes".format(size))
            for idx in range(copies):
                f = generateRandomFile(size)
                fileDID = '{}:{}'.format(scope, os.path.basename(f.name))
                self.logger.debug("    Uploading file {} of {}".format(
                    idx+1, copies))
                rucio.upload(rse=rses[0], scope=scope, filePath=f.name)
                self.logger.debug("      Attaching file {} to {}".format(
                    fileDID, datasetDID))
                rucio.attach(scope=scope, todid=datasetDID, dids=fileDID)
            self.logger.info("Upload complete")

            self.logger.info("Adding replication rules...")
            for rse in rses:
                self.logger.info("RSE: {}".format(rse))
                rtn = rucio.addRule(fileDID, str(copies), rse)
                self.logger.debug("  {}".format(rtn.stdout.decode('UTF-8').rstrip('\n')))
            self.logger.info("Replication rules added")

        self.toc()
        self.logger.info("Finished in {}s".format(
            round(self.elapsed)))