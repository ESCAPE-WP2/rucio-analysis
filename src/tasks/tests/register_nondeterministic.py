import os
from common.rucio.helpers import createCollection

from gfal2 import Gfal2Context

from common.rucio.wrappers import RucioWrappersAPI
from common.rucio.pfn import PFN
from tasks.task import Task


class TestRegisterNondeterministic(Task):
    """ Register uploaded nondeterministic data from a filelist. """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            # Assign variables from kwargs.
            #
            rse = kwargs["rse"]
            scheme = kwargs["scheme"]
            hostname = kwargs["hostname"]
            prefix = kwargs["prefix"]
            filelistDir = kwargs["filelist_dir"]
            lifetime = kwargs["lifetime"]
            datasetName = kwargs["dataset_name"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        # Set up GFAL2 context.
        #
        gfal = Gfal2Context()

        # Verify the scheme, hostname and prefix (protocol) is supported by this RSE.
        #
        rucio = RucioWrappersAPI()
        selectedProtocol = None
        for protocol in rucio.getRSEProtocols(rse):
            if (
                protocol["scheme"] == scheme
                and protocol["hostname"] == hostname
                and protocol["prefix"] == prefix
            ):
                selectedProtocol = "{}://{}{}".format(scheme, hostname, prefix)
                break

        if not selectedProtocol:
            self.logger.critical("Protocol not supported by this RSE.")
            return False

        # Open each ingest file list and get the files to register.
        #
        filelistDirAbsPath = os.path.join(selectedProtocol, filelistDir)
        for filelist in gfal.listdir(filelistDirAbsPath):
            if filelist in [".", ".."] or filelist.startswith("."):
                continue
            filelistAbsPath = os.path.join(filelistDirAbsPath, filelist)
            ingestFilelist_p = gfal.open(filelistAbsPath, "r")
            filelistContents = (
                ingestFilelist_p.read(gfal.stat(filelistAbsPath).st_size)
                .rstrip("\r\n")
                .split("\n")
            )

            # Then register the file and add expiration rule.
            #
            for entry in filelistContents:
                if entry.startswith("#"):
                    headers = tuple(entry.lstrip("#").split())
                    if headers != ("pfn", "did"):
                        self.logger.critical("Ingest file format not compliant.")
                        return False
                    continue

                pfn, did = entry.split("\t")
                pfn = PFN.fromabspath(pfn)

                self.logger.info(
                    "Adding replica for {} at {} at with did {}".format(
                        pfn.name, rse, did
                    )
                )

                rucio.addReplica(rse, did, pfn.abspath)
                datasetDID = createCollection(
                    self.logger.name, did.split(":")[0], datasetName
                )
                self.logger.info("Attaching did to dataset {}".format(datasetDID))
                rucio.attach(datasetDID, did)
                self.logger.info(
                    "Adding rule to keep it there for {}s".format(lifetime)
                )
                rucio.addRule(did, 1, rse, lifetime)

            # this is not really "safe" as we can end up with partially processed files
            gfal.unlink(filelistAbsPath)

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
