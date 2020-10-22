from datetime import datetime

from elasticsearch import Elasticsearch

from rucio_helper import create_did
from rucio_wrappers import RucioWrappersAPI
from utility import bcolors, generateRandomFile
from tests import Test


class TestReplication(Test):
    """ Rucio file upload/replication to a list of RSEs. """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            # Assign variables from test.yml kwargs.
            #
            nFiles = kwargs["n_files"]
            rses = kwargs["rses"]
            scope = kwargs["scope"]
            lifetime = kwargs["lifetime"]
            sizes = kwargs["sizes"]
            databases = kwargs['databases']
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for test.")
            self.logger.critical(repr(e))
            exit()

        # Instantiate RucioWrappers class to allow access to static methods.
        #
        rucio = RucioWrappersAPI()

        # Create a dataset to house the data, named with today's date
        # and scope <scope>.
        #
        datasetDID = create_did(self.logger.name, scope)

        # Iteratively upload a file of size from <sizes> to each
        # RSE, attach to the dataset, add replication rules to the
        # other listed RSEs.
        #
        for rseSrc in rses:
            self.logger.info(
                bcolors.OKBLUE + "RSE (src): {}".format(rseSrc) + bcolors.ENDC
            )
            for size in sizes:
                self.logger.debug("  File size: {} bytes".format(size))
                for idx in range(nFiles):
                    # Generate random file of size <size>
                    f = generateRandomFile(size)
                    fileDID = "{}:{}".format(scope, os.path.basename(f.name))

                    # Upload to <rseSrc>
                    self.logger.debug(
                        "    Uploading file {} of {}".format(idx + 1, nFiles)
                    )
                    try:
                        rucio.upload(
                            rse=rseSrc, scope=scope, filePath=f.name, lifetime=lifetime
                        )
                    except Exception as e:
                        self.logger.warning(repr(e))
                        os.remove(f.name)
                        break
                    self.logger.debug("    Upload complete")
                    os.remove(f.name)

                    # Attach to dataset
                    self.logger.debug(
                        "    Attaching file {} to {}".format(fileDID, datasetDID)
                    )
                    try:
                        rucio.attach(todid=datasetDID, dids=fileDID)
                    except Exception as e:
                        self.logger.warning(repr(e))
                        break
                    self.logger.debug("    Attached file to dataset")

                    # Add replication rules for other RSEs
                    self.logger.debug("    Adding replication rules...")
                    for rseDst in rses:
                        if rseSrc == rseDst:
                            continue
                        self.logger.debug(
                            bcolors.OKGREEN
                            + "    RSE (dst): {}".format(rseDst)
                            + bcolors.ENDC
                        )
                        try:
                            rtn = rucio.addRule(fileDID, 1, rseDst, lifetime=lifetime)
                            self.logger.debug("      Rule ID: {}".format(rtn))
                        except Exception as e:
                            self.logger.warning(repr(e))
                            continue
                    self.logger.debug("    Replication rules added")

                    for database in databases:
                        if database['type'] == 'es':
                            self.logger.debug("    Injecting rules into ES database...")
                            uri = database['uri']
                            index = database['index']
                            rules = rucio.listReplicationRules(fileDID)
                            for rule in rules:
                                try:
                                    entry = {
                                        'rule_id': rule['id'],
                                        'scope': rule['scope'],
                                        'name': rule['name'],
                                        'from_rse': rseSrc,
                                        'to_rse': rule['rse_expression'],
                                        'created_at': rule['created_at'],
                                        'updated_at': rule['updated_at'],
                                        'expires_at': rule['expires_at'],
                                        'state': rule['state'],
                                        'error': rule['error']
                                    }
                                    entry['is_done'] = 1 if entry['state'] == \
                                        'OK' else 0
                                    entry['is_replicating'] = 1 if entry['state'] == \
                                        'REPLICATING' else 0
                                    entry['is_stuck'] = 1 if entry['state'] == \
                                        'STUCK' else 0
                                    entry['is_submitted'] = 1

                                    try:
                                        replica = rucio.listFileReplicas(
                                            fileDID, rse=entry['to_rse'])
                                        protocol = replica[0]['rses'][entry[
                                            'to_rse']][0].split(':')[0]
                                        endpoint = replica[0]['rses'][entry[
                                            'to_rse']][0]
                                    except KeyError:
                                        endpoint = None
                                        protocol = None

                                    entry['endpoint'] = endpoint
                                    entry['protocol'] = protocol

                                    entry['@timestamp'] = int(
                                        datetime.now().strftime("%s"))*1000

                                    es = Elasticsearch([uri])
                                    es.index(index=index, id=entry['rule_id'], 
                                        body=entry)
                                except Exception as e:
                                    self.logger.warning(repr(e))
                                    continue

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
