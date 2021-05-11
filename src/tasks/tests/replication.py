from common.es.rucio import Rucio as ESRucio
from common.rucio.wrappers import RucioWrappersAPI
from tasks.task import Task
from utility import bcolors


class TestReplication(Task):
    """ Rucio DID replication to a list of RSEs. """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            dids = kwargs["dids"]
            rses = kwargs["rses"]
            lifetime = kwargs["lifetime"]
            databases = kwargs["databases"]
            taskName = kwargs["task_name"]
            asynchronous = kwargs.get("asynchronous", False)
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        # Instantiate RucioWrappers class to allow access to static methods.
        #
        rucio = RucioWrappersAPI()

        for did in dids:
            self.logger.debug("Adding replication rules...")
            for rse in rses:
                self.logger.debug(
                    bcolors.OKGREEN + "RSE (dst): {}".format(rse) + bcolors.ENDC
                )
                try:
                    rtn = rucio.addRule(
                        did, 1, rse, lifetime=lifetime, asynchronous=asynchronous
                    )
                    self.logger.debug("Rule ID: {}".format(rtn[0]))
                except Exception as e:
                    self.logger.warning(repr(e))
                    continue
                self.logger.debug("Replication rules added")

                # Get information about the DID: size and did_type
                metadata = rucio.getMetadata(did)

                # Push corresponding rules to database
                for database in databases:
                    if database["type"] == "es":
                        self.logger.debug("Injecting rules into ES database...")
                        es = ESRucio(database["uri"], self.logger)
                        es.pushRulesForDID(
                            did,
                            index=database["index"],
                            baseEntry={
                                "task_name": taskName,
                                "file_size": metadata["bytes"],
                                "type": metadata["did_type"].lower(),
                                "n_files": 1,
                                "is_submitted": 1,
                            },
                        )

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
