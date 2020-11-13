from multiprocessing import Pool
import os
import random
import time
import uuid

from db import ES
from rucio_helper import createDID, uploadDirReplicate
from rucio_wrappers import RucioWrappersAPI, RucioWrappersCLI
from tasks import Task
from utility import bcolors, generateRandomFile


class TestUploadReplication(Task):
    """ Rucio file upload/replication to a list of RSEs. """

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
            databases = kwargs["databases"]
            taskName = kwargs["task_name"]
            data_tag = kwargs.get("data_tag", "")
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

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
            for size in sizes:
                self.logger.debug("File size: {} bytes".format(size))
                for idx in range(nFiles):
                    # Generate random file of size <size>
                    f = generateRandomFile(size, prefix=data_tag)
                    fileDID = "{}:{}".format(scope, os.path.basename(f.name))

                    # Upload to <rseSrc>
                    self.logger.debug("Uploading file {} of {}".format(idx + 1, nFiles))
                    try:
                        rucio.upload(
                            rse=rseSrc, scope=scope, filePath=f.name, lifetime=lifetime
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

                    # Add replication rules for other RSEs
                    self.logger.debug("Adding replication rules...")
                    for rseDst in rses:
                        if rseSrc == rseDst:
                            continue
                        self.logger.debug(
                            bcolors.OKGREEN
                            + "RSE (dst): {}".format(rseDst)
                            + bcolors.ENDC
                        )
                        try:
                            rtn = rucio.addRule(
                                fileDID, 1, rseDst, lifetime=lifetime, src=rseSrc
                            )
                            self.logger.debug("Rule ID: {}".format(rtn[0]))
                        except Exception as e:
                            self.logger.warning(repr(e))
                            continue
                    self.logger.debug("Replication rules added")

                    # Push corresponding rules to database
                    for database in databases:
                        if database["type"] == "es":
                            self.logger.debug("Injecting rules into ES database...")
                            es = ES(database["uri"], self.logger)
                            es.pushRulesForDID(
                                fileDID,
                                index=database["index"],
                                baseEntry={
                                    "task_name": taskName,
                                    "file_size": size,
                                    "type": "file",
                                    "n_files": 1,
                                    "is_submitted": 1,
                                },
                            )

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))


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
            container_name = kwargs.get("container_name", None)
            data_tag = kwargs.get("data_tag", "")
            databases = kwargs["databases"]
            taskName = kwargs["task_name"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        loggerName = self.logger.name

        # Create a DID to group the data, by default named with today's date
        # and scope <scope>.
        #
        if container_name is None:
            parentDID = createDID(loggerName, scope, "CONTAINER")
        else:
            container_DID = "{}:{}".format(scope, container_name)
            parentDID = createDID(loggerName, scope, "CONTAINER", container_DID)

        self.logger.debug("Launching pool of {} workers".format(nWorkers))

        # Create array of args for each process
        #
        args_arr = [
            (
                taskName,
                loggerName,
                databases,
                rseSrc,
                rsesDst,
                nFiles,
                fileSize,
                scope,
                lifetime,
                parentDID,
                dirIdx,
                nDirs,
                data_tag,
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


class TestReplicationQos(Task):
    """ Test replication for full grid of available QoS labels. """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            qos = kwargs["qos"]
            scope = kwargs["scope"]
            lifetimes = kwargs["lifetimes"]
            size = kwargs["size"]
            databases = kwargs["databases"]
            taskName = kwargs["task_name"]
            data_tag = kwargs.get("data_tag", "")
            if len(qos) != len(lifetimes):
                self.logger.critical(
                    "{} qos and {} lifetimes passed. "
                    "Expected the same number of each arg.".format(
                        len(qos), len(lifetimes)
                    )
                )
                return False
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        # Instantiate RucioWrappers to make Rucio calls
        #
        rucio_api = RucioWrappersAPI()
        rucio_cli = RucioWrappersCLI()

        # Create a dataset to house the data, named with today's date
        # and scope <scope>.
        #
        datasetDID = createDID(self.logger.name, scope)
        self.logger.debug("datasetDID: {}".format(datasetDID))

        # Unable to upload with a QoS flag currently; therefore choose a source RSE
        # at random from RSEs with the <qos> label.
        qos_src = qos.pop(0)
        life_src = lifetimes.pop(0)
        try:
            source_rse = random.choice(
                [rse["rse"] for rse in rucio_api.listRSEs("QOS={}".format(qos_src))]
            )
        except Exception as e:
            self.logger.critical(repr(e))
            return False

        f = generateRandomFile(size, prefix=data_tag)
        fileDID = "{}:{}".format(scope, os.path.basename(f.name))

        # Upload to <rseSrc>
        self.logger.debug(
            "Uploading file {} to RSE {} (QoS: {})".format(f.name, source_rse, qos_src)
        )
        try:
            rucio_cli.upload(
                rse=source_rse, scope=scope, filePath=f.name, lifetime=life_src
            )
            self.logger.debug("Upload complete")
            os.remove(f.name)
        except Exception as e:
            self.logger.warning("Upload failed; ({})".format(repr(e)))
            os.remove(f.name)
            return False

        # Attach to dataset
        self.logger.debug("Attaching file {} to {}".format(fileDID, datasetDID))
        try:
            rucio_cli.attach(todid=datasetDID, dids=fileDID)
            self.logger.debug("Attached file to dataset")
        except Exception as e:
            self.logger.warning(repr(e))

        # Add replication rules for destination QoS
        self.logger.debug("Adding QoS-based replication rules...")

        for i, qos_dest in enumerate(qos):
            self.logger.debug(
                "Replicate to destination with QOS {} with lifetime {} sec".format(
                    qos_dest, lifetimes[i]
                )
            )
            try:
                rtn = rucio_cli.addRuleWithOptions(
                    fileDID,
                    1,
                    "QOS={}".format(qos_dest),
                    lifetime=lifetimes[i],
                    activity="Debug",
                    src="QOS={}".format(qos_src),
                )
                self.logger.debug(
                    "Rule ID: {}".format(rtn.stdout.decode("UTF-8").rstrip("\n"))
                )
            except Exception as e:
                self.logger.warning(repr(e))
                continue

        # Push corresponding rules to database
        for database in databases:
            if database["type"] == "es":
                self.logger.debug("Injecting rules into ES database...")
                es = ES(database["uri"], self.logger)
                es.pushRulesForDID(
                    fileDID,
                    index=database["index"],
                    baseEntry={
                        "task_name": taskName,
                        "file_size": size,
                        "type": "file",
                        "n_files": 1,
                        "is_submitted": 1,
                    },
                )

        self.logger.debug("Replication rules added for source QoS {}".format(qos_src))

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))


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
            protocols = kwargs["protocols"]
            databases = kwargs["databases"]
            taskName = kwargs["task_name"]
            data_tag = kwargs.get("data_tag", "")
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

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
        for rseDst in rses:
            self.logger.info(
                bcolors.OKBLUE + "RSE (dst): {}".format(rseDst) + bcolors.ENDC
            )
            for protocol in protocols:
                for size in sizes:
                    self.logger.debug("File size: {} bytes".format(size))
                    for idx in range(nFiles):
                        # Generate random file of size <size>
                        f = generateRandomFile(size, prefix=data_tag)
                        fileDID = "{}:{}".format(scope, os.path.basename(f.name))

                        # Upload to <rseDst>
                        self.logger.debug(
                            "Uploading file {} of {} with protocol {}".format(
                                idx + 1, nFiles, protocol
                            )
                        )

                        entry = {
                            "task_name": taskName,
                            "file_size": size,
                            "type": "file",
                            "n_files": 1,
                            "is_upload_submitted": 1,
                        }
                        try:
                            st = time.time()
                            rucio.upload(
                                rse=rseDst,
                                scope=scope,
                                filePath=f.name,
                                lifetime=lifetime,
                                forceScheme=protocol,
                            )
                            entry["upload_duration"] = time.time() - st
                            entry["state"] = "UPLOAD-SUCCESSFUL"
                            self.logger.debug("Upload complete")

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
                        except Exception as e:
                            self.logger.warning("Upload failed: {}".format(e))
                            entry["rule_id"] = (str(uuid.uuid4()),)
                            entry["scope"] = (scope,)
                            entry["name"] = (os.path.basename(f.name),)
                            entry["to_rse"] = (rseDst,)
                            entry["error"] = repr(e.__class__.__name__).strip("'")
                            entry["protocol"] = protocol
                            entry["state"] = "UPLOAD-FAILED"
                        os.remove(f.name)

                        # Push corresponding rules to database
                        for database in databases:
                            if database["type"] == "es":
                                self.logger.debug("Injecting rules into ES database...")
                                es = ES(database["uri"], self.logger)
                                es.pushRulesForDID(
                                    fileDID, index=database["index"], baseEntry=entry
                                )

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))


class TestReplication(Task):
    """ Rucio DID replication to a list of RSEs. """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            DIDs = kwargs["dids"]
            rses = kwargs["rses"]
            lifetime = kwargs["lifetime"]
            databases = kwargs["databases"]
            taskName = kwargs["task_name"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        # Instantiate RucioWrappers class to allow access to static methods.
        #
        rucio = RucioWrappersAPI()

        for DID in DIDs:
            self.logger.debug("Adding replication rules...")
            for rse in rses:
                self.logger.debug(
                    bcolors.OKGREEN + "RSE (dst): {}".format(rse) + bcolors.ENDC
                )
                try:
                    rtn = rucio.addRule(DID, 1, rse, lifetime=lifetime)
                    self.logger.debug("Rule ID: {}".format(rtn[0]))
                except Exception as e:
                    self.logger.warning(repr(e))
                    continue
                self.logger.debug("Replication rules added")

                # Get information about the DID: size and did_type
                metadata = rucio.getMetadata(DID)

                # Push corresponding rules to database
                for database in databases:
                    if database["type"] == "es":
                        self.logger.debug("Injecting rules into ES database...")
                        es = ES(database["uri"], self.logger)
                        es.pushRulesForDID(
                            DID,
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