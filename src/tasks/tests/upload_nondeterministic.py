import os
import gfal2
from datetime import datetime
from rucio_wrappers import RucioWrappersAPI
from rucio_helpers import createPFN
from tasks.task import Task
from utility import generateRandomFile

from gfal2 import (
    Gfal2Context,
    GError,
)


class TestUploadNondeterministic(Task):
    
    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            # Assign variables from kwargs.
            #
            nFiles = kwargs["n_files"]
            nProjects = kwargs["n_projects"]
            nDatasets = kwargs["n_datasets"]
            size = kwargs["sizes"]
            rse = kwargs["rse"]
            scope = kwargs["scope"]
            lifetime = kwargs["lifetime"]
            test_dir_prefix = kwargs["test_dir_prefix"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        self.logger.info("start non deterministic upload test")
        gfal = Gfal2Context()
        params = gfal.transfer_parameters()
        params.set_checksum = True
        params.overwrite = True
        params.set_create_parent = True
        params.get_create_parent = True
        params.timeout = 300

        rucio = RucioWrappersAPI()
        
        # Paths for data uploaded are constructed as below
        # rse_prefix/scope/test_dir_prefix_<timestamp>/project<Idx>/dataset<Idy>/filename<Idz>_timestamp
        
        test_dir = test_dir_prefix + "_" + datetime.now().strftime("%d%m%yT%H.%M.%S")

        # Create test dir
        test_dir_pfn = createPFN(self.logger.name, rse, scope, test_dir)
        gfal.mkdir_rec(test_dir_pfn, 775)
        for p in range(0, nProjects):
            # Create project dir
            project_dir_pfn = os.path.join(test_dir_pfn, "project" + str(p))
            gfal.mkdir_rec(project_dir_pfn, 775)
            for d in range(0, nDatasets):
                # Create dataset dir
                dataset_dir_pfn = os.path.join(project_dir_pfn, "dataset" + str(d))
                gfal.mkdir_rec(dataset_dir_pfn, 775)
                for f in range(0, nFiles):
                    # Create file
                    f = generateRandomFile(size[0])
                    did = "{}:{}".format(scope, os.path.basename(f.name))
                    pfn = os.path.join(dataset_dir_pfn, os.path.basename(f.name))
                    
                    self.logger.info("Uploading file with pfn {}".format(pfn))
                    gfal.filecopy(params, "file://" + f.name, pfn)

                    self.logger.info(
                        "Adding replica at {} with name {}".format(
                            rse, os.path.basename(f.name)
                        )
                    )

                    rucio.addReplica(gfal, rse, did, pfn)
                    self.logger.info(
                        "Adding rule to keep it there for {} sec".format(lifetime)
                    )
                    rucio.addRule(did, 1, rse, lifetime)
                    os.remove(f.name)

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
