from elasticsearch import Elasticsearch

from rucio_wrappers import RucioWrappersAPI
from tests import Test


class UpdateDatabaseReplication(Test):
    """ Update ES database with replication statuses. """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            # Assign variables from test.yml kwargs.
            #
            databaseType = kwargs['database']['type']
            databaseUri = kwargs['database']['uri']
            databaseIndex = kwargs['database']['index'] 
            databaseSearchRangeLTE = kwargs['database']['search_range_lte']
            databaseSearchRangeGTE = kwargs['database']['search_range_gte']
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for test.")
            self.logger.critical(repr(e))
            exit()

        # Instantiate RucioWrappers class to allow access to static methods.
        #
        rucio = RucioWrappersAPI()

        if databaseType == 'es':
            es = Elasticsearch([databaseUri])
            res = es.search(index=databaseIndex, size=1000, body={
                "query": {
                    "bool": {
                        "must": [{
                            "term": {
                                "is_done": 0
                            }
                        }, {
                            "range": {
                                "created_at": {
                                    "gte": databaseSearchRangeGTE,
                                    "lte": databaseSearchRangeLTE
                                }
                            }
                        }]
                    }
                }
            })
            for hit in res['hits']['hits']:
                rule_id = hit['_source']['rule_id']
                rule = rucio.ruleInfo(rule_id)
                entry = {
                    'scope': rule['scope'],
                    'name': rule['name'],
                    'to_rse': rule['rse_expression'],
                    'updated_at': rule['updated_at'],
                    'expires_at': rule['expires_at'],
                    'state': rule['state'],
                    'error': rule['error']
                }
                entry['is_done'] = 1 if entry['state'] == 'OK' else 0
                entry['is_replicating'] = 1 if entry['state'] == 'REPLICATING' else 0
                entry['is_stuck'] = 1 if entry['state'] == 'STUCK' else 0

                did = "{}:{}".format(entry['scope'], entry['name'])
                try:
                    replica = rucio.listFileReplicas(did, rse=entry['to_rse'])
                    protocol = replica[0]['rses'][entry['to_rse']][0].split(':')[0]
                    endpoint = replica[0]['rses'][entry['to_rse']][0]
                except KeyError:
                    endpoint = None
                    protocol = None

                entry['endpoint'] = endpoint
                entry['protocol'] = protocol

                res = es.update(index='[replication]', id=rule_id, body={
                    "doc": entry
                })

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
