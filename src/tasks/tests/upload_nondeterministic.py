import os
import time

from gfal2 import Gfal2Context
import importlib

from rucio.wrappers import RucioWrappersAPI
from tasks.task import Task


class TestUploadNondeterministic(Task):
    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            # Assign variables from kwargs.
            #
            rse = kwargs["rse"]
            scope = kwargs["scope"]
            lifetime = kwargs["lifetime"]
            scheme = kwargs["scheme"]
            lfn2pfnClassName = kwargs["lfn2pfn_class_name"]
            lfn2pfnKwargs = kwargs["lfn2pfn_kwargs"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        # Set up GFAL2 context and transfer parameters.
        #
        gfal = Gfal2Context()
        params = gfal.transfer_parameters()
        params.set_checksum = True
        params.overwrite = True
        params.set_create_parent = True
        params.get_create_parent = True
        params.timeout = 300

        # Get the RSE scheme, hostname and prefix.
        #
        rucio = RucioWrappersAPI()
        protocols = rucio.getRSEProtocols(rse)
        if scheme != 'first':
            for protocol in protocols:
                if protocol['scheme'] == scheme:
                    hostname = protocol['hostname']
                    rse_prefix = protocol['prefix']
                    break
        else:
            scheme = protocol["scheme"]
            hostname = protocol["hostname"]
            rse_prefix = protocol["prefix"]

        # Instantiate the requested lfn2pfn class.
        #
        try:
            lfn2pfn = getattr(importlib.import_module(
                "rucio.lfn2pfn"), lfn2pfnClassName)
            (self.logger, scheme, hostname, rse_prefix, scope, lfn2pfnKwargs)
        except ImportError as e:
            self.logger.critical("Could not import lfn2pfn module.")
            self.logger.critical(repr(e))
            return False
        except AttributeError as e:
            self.logger.critical("Requested class not found in lfn2pfn module.")
            self.logger.critical(repr(e))
            exit()

        # Upload.
        #
        st = time.time()
        for directory, filename, pfn in zip(
                lfn2pfn.directories, lfn2pfn.filenames, lfn2pfn.pfns):
            gfal.mkdir_rec(os.path.dirname(directory), 775)

            self.logger.info("Uploading file with pfn {}".format(pfn))
            gfal.filecopy(params, "file://" + filename, pfn)
        self.logger.info("Finished upload in {}s".format(
            round(time.time() - st, 3)))

        # Add replica and expiration rule.
        #
        st = time.time()
        for did, pfn in zip(lfn2pfn.dids, lfn2pfn.pfns):
            self.logger.info(
                "Adding replica at {} with name {}".format(rse, filename)
            )
            rucio.addReplica(gfal, rse, did, pfn)
            self.logger.info(
                "Adding rule to keep it there for {}s".format(lifetime)
            )
            rucio.addRule(did, 1, rse, lifetime)
        self.logger.info("Finished adding replicas in {}s".format(
            round(time.time() - st, 3)))

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
