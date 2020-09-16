from datetime import datetime
import os

from rucio import Rucio
from tests import Test
from utility import generateRandomFile, bcolors

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
            nFiles = kwargs['n_files']
            rses = kwargs['rses']
            scope = kwargs['scope']
            lifetime = kwargs['lifetime']
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
        todaysDate = datetime.now().strftime('%d-%m-%Y')
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

        # Recursively upload a file of size from <sizes> to each
        # RSE, attach to the dataset, add add replication rules to the 
        # other listed RSEs.
        #
        for rseSrc in rses:
            self.logger.info(bcolors.OKBLUE + "RSE (src): {}".format(rseSrc) + bcolors.ENDC)
            self.logger.info("Starting upload...")
            for size in sizes:
                self.logger.debug("  File size: {} bytes".format(size))
                for idx in range(nFiles):
                    # Generate random file of size <size>
                    f = generateRandomFile(size)
                    fileDID = '{}:{}'.format(scope, os.path.basename(f.name))

                    # Upload to <rseSrc>
                    self.logger.debug("    Uploading file {} of {}".format(
                        idx+1, nFiles))
                    try:
                        rucio.upload(rse=rseSrc, scope=scope, filePath=f.name, 
                            lifetime=lifetime)
                    except Exception as e:
                        self.logger.warning(repr(e))
                        os.remove(f.name)
                        break
                    self.logger.debug("      Attaching file {} to {}".format(
                        fileDID, datasetDID))
                    self.logger.info("Upload complete")
                    os.remove(f.name)

                    # Attach to dataset
                    try:
                        rucio.attach(scope=scope, todid=datasetDID, dids=fileDID)
                    except Exception as e:
                        self.logger.warning(repr(e))
                        break
                    self.logger.info("Attached to dataset")

                    # Add replication rules for other RSEs
                    self.logger.info("Adding replication rules...")
                    for rseDst in rses:
                        if rseSrc == rseDst:
                            continue
                        self.logger.info(
                            bcolors.OKGREEN + "  RSE (dst): {}".format(rseDst) + \
                                bcolors.ENDC)
                        try:
                            rtn = rucio.addRule(fileDID, 1, rseDst, lifetime=lifetime)
                            self.logger.debug("    {}".format(
                                rtn.stdout.decode('UTF-8').rstrip('\n')))
                        except Exception as e:
                            self.logger.warning(repr(e))
                            continue
                    self.logger.info("Replication rules added")

        self.toc()
        self.logger.info("Finished in {}s".format(
            round(self.elapsed)))