from datetime import datetime
import os
import shutil

from rucio import Rucio
from tests import Test
from utility import generateDirRandomFiles, bcolors

class TestReplicateDir(Test):
    """
    Rucio upload/replicate directory of files to a list of RSEs.
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
            file_size = kwargs['file_size']
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

        # Upload a dir containing <nFiles> of <file_size> to each
        # RSE, attach to the dataset, add replication rules to the 
        # other listed RSEs.
        #
        for rseSrc in rses:
            self.logger.info(bcolors.OKBLUE + "RSE (src): {}".format(rseSrc) + bcolors.ENDC)

            # Generate directory:
            dirPath = generateDirRandomFiles(nFiles, file_size)

            # Upload to <rseSrc>
            self.logger.debug("    Uploading dir {} and attaching to {}".format(dirPath, datasetDID))

            try:
                rucio.upload_dir(rseSrc, scope, dirPath, lifetime, datasetDID)
            except Exception as e:
                self.logger.warning(repr(e))
                shutil.rmtree(dirPath)
                break
            self.logger.debug("    Upload complete")

            # Add replication rules for other RSEs
            for filename in os.listdir(dirPath):
                fileDID = '{}:{}'.format(scope, filename)
                self.logger.debug("    Adding replication rule for {}".format(fileDID))
                for rseDst in rses:
                    if rseSrc == rseDst:
                        continue
                    self.logger.debug(
                        bcolors.OKGREEN + "    RSE (dst): {}".format(rseDst) + \
                            bcolors.ENDC)
                    try:
                        rtn = rucio.addRule(fileDID, 1, rseDst, lifetime=lifetime)
                        self.logger.debug("      Rule ID: {}".format(
                            rtn.stdout.decode('UTF-8').rstrip('\n')))
                    except Exception as e:
                        self.logger.warning(repr(e))
                        continue
            self.logger.debug("    All replication rules added")
            shutil.rmtree(dirPath)

        self.toc()
        self.logger.info("Finished in {}s".format(
            round(self.elapsed)))
