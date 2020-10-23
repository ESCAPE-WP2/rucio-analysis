from datetime import datetime

from elasticsearch import Elasticsearch

from rucio_wrappers import RucioWrappersAPI


class ES():
    def __init__(self, uri, logger):
        self.es = Elasticsearch([uri])
        self.rucio = RucioWrappersAPI()
        self.logger = logger

    def _index(self, index, documentID, body):
        try:
            self.es.index(index=index, id=documentID, body=body)
        except Exception as e:
            self.logger.warning("Failed to index: {}".format(e))
            exit()

    def _update(self, index, documentID, body):
        try:
            res = self.es.update(index=index, id=documentID, body=body)
        except Exception as e:
            self.logger.warning("Failed to update database: {}".format(e))

    def pushRulesForDID(self, did, index, extraEntries={}):
        rules = self.rucio.listReplicationRules(did)
        for rule in rules:
            try:
                entry = {
                    'rule_id': rule['id'],
                    'scope': rule['scope'],
                    'name': rule['name'],
                    'from_rse': rule['source_replica_expression'],
                    'to_rse': rule['rse_expression'],
                    'created_at': rule['created_at'],
                    'updated_at': rule['updated_at'],
                    'expires_at': rule['expires_at'],
                    'state': rule['state'],
                    'error': rule['error']
                }
                entry = {**entry, **extraEntries}

                entry['is_done'] = 1 if entry['state'] == \
                    'OK' else 0
                entry['is_replicating'] = 1 if entry['state'] == \
                    'REPLICATING' else 0
                entry['is_stuck'] = 1 if entry['state'] == \
                    'STUCK' else 0
                entry['is_submitted'] = 1

                try:
                    replica = self.rucio.listFileReplicas(did, rse=entry['to_rse'])
                    protocol = replica[0]['rses'][entry['to_rse']][0].split(':')[0]
                    endpoint = replica[0]['rses'][entry['to_rse']][0]
                except KeyError:    # if the rule doesn't exist
                    endpoint = None
                    protocol = None
                entry['endpoint'] = endpoint
                entry['protocol'] = protocol

                entry['@timestamp'] = int(datetime.now().strftime("%s"))*1000

                self._index(index=index, documentID=entry['rule_id'], body=entry)
            except Exception as e:
                self.logger.warning("Failed to push rule: {}".format(e))
                continue

    def search(self, index, body, maxRows=1000):
        try:
            res = self.es.search(index=index, size=maxRows, body=body)
            return res
        except Exception as e:
            self.logger.critical("Failed to complete search: {}".format(e))
            exit()

    def updateRuleWithDID(self, ruleID, index, extraEntries={}):
        try:
            self.logger.debug("Getting rule information...")
            rule = self.rucio.ruleInfo(ruleID)
        except Exception as e:
            self.logger.warning("Error getting rule information, " +
                "skipping: {}".format(repr(e)))
            return

        # Form JSON to inject as update body.
        #
        entry = {
            'scope': rule['scope'],
            'name': rule['name'],
            'to_rse': rule['rse_expression'],
            'updated_at': rule['updated_at'],
            'expires_at': rule['expires_at'],
            'state': rule['state'],
            'error': rule['error']
        }
        entry = {**entry, **extraEntries}

        entry['is_done'] = 1 if entry['state'] == 'OK' else 0
        entry['is_replicating'] = 1 if entry['state'] == 'REPLICATING' else 0
        entry['is_stuck'] = 1 if entry['state'] == 'STUCK' else 0

        did = "{}:{}".format(entry['scope'], entry['name'])
        try:
            replica = self.rucio.listFileReplicas(did, rse=entry['to_rse'])
            protocol = replica[0]['rses'][entry['to_rse']][0].split(':')[0]
            endpoint = replica[0]['rses'][entry['to_rse']][0]
        except KeyError:    # if the rule doesn't exist
            endpoint = None
            protocol = None
        entry['endpoint'] = endpoint
        entry['protocol'] = protocol

        self.logger.info("Updating rule...")
        self._update(index=index, documentID=ruleID, body={"doc": entry})
        self.logger.info("Update complete")