from datetime import datetime

from elasticsearch import Elasticsearch

from rucio_wrappers import RucioWrappersAPI


class ES():
    """
    Class providing common functionality for interaction with elasticsearch
    backends.
    """

    def __init__(self, uri, logger):
        self.es = Elasticsearch([uri])
        self.logger = logger

    def _get(self, index, documentID):
        """ Get a document from index, <index>, with document ID, <documentID>. """
        try:
            return self.es.get(index=index, id=documentID)
        except Exception as e:
            self.logger.warning("Failed to get document: {}".format(e))

    def _index(self, index, documentID, body):
        """ Create new document with id, <documentID>, in index, <index>. """
        try:
            self.es.index(index=index, id=documentID, body=body)
        except Exception as e:
            self.logger.critical("Failed to index: {}".format(e))
            return False

    def _search(self, index, body, maxRows):
        """ Search an index, <index>. """
        try:
            res = self.es.search(index=index, size=maxRows, body=body)
            return res
        except Exception as e:
            self.logger.critical("Failed to complete search: {}".format(e))
            exit()

    def _update(self, index, documentID, body):
        """ Update an existing document with id, <documentID>, in index, <index>. """
        try:
            self.es.update(index=index, id=documentID, body=body)
        except Exception as e:
            self.logger.warning("Failed to update database: {}".format(e))

    def search(self, index, body, maxRows=1000):
        return self._search(index, body, maxRows)


class ESRucio(ES):
    """
    Class bulding upon ES base class, but providing additional functionality for
    pushing Rucio related information to an elasticsearch index.
    """

    def pushRulesForDID(self, did, index, baseEntry={}):
        """ Create documents in the database corresponding to replication rules for
            a given DID < did > and add to index < index > . < baseEntry > key/value
            pairs will be appended to all entries prior to submission.
        """
        rucio = RucioWrappersAPI()
        rules = rucio.listReplicationRules(did)
        entries = []
        if len(rules) > 0:              # if this DID has replication rules ...
            for rule in rules:
                entry = {
                    '@timestamp': int(datetime.now().strftime("%s"))*1000,
                    'rule_id': rule['id'],
                    'scope': rule['scope'],
                    'name': rule['name'],
                    'from_rse': rule['source_replica_expression'],
                    'to_rse': rule['rse_expression'],
                    'created_at': rule['created_at'],
                    'updated_at': rule['updated_at'],
                    'expires_at': rule['expires_at'],
                    'state': rule['state'],
                    'error': rule['error'],
                }

                # Append protocol and endpoint details to entry using result from
                # a separate call to list file replicas for this particular DID.
                #
                try:
                    replica = rucio.listFileReplicas(did, rse=entry['to_rse'])
                    protocol = replica[0]['rses'][entry['to_rse']][0].split(':')[0]
                    endpoint = replica[0]['rses'][entry['to_rse']][0]
                except Exception:    # if the rule doesn't exist
                    endpoint = None
                    protocol = None
                entry['endpoint'] = endpoint
                entry['protocol'] = protocol

                entries.append(entry)
        else:                           # otherwise, create entry with only timestamp
            now = datetime.now()
            entries.append({
                '@timestamp': int(now.strftime("%s"))*1000,
            })

        for entry in entries:
            # The <fullEntry> pushed to the database is the concatenation of the two
            # dictionaries, <entry>, generated as above, and 'baseEntry', described
            # in the function definition.
            #
            fullEntry = {**entry, **baseEntry}

            # Add boolean flags for state. This makes it easy to use bucket
            # aggregations in ES queries.
            #
            try:
                fullEntry['is_done'] = 1 if fullEntry['state'] == 'OK' else 0
                fullEntry['is_replicating'] = 1 if fullEntry['state'] \
                    == 'REPLICATING' else 0
                fullEntry['is_stuck'] = 1 if fullEntry['state'] \
                    == 'STUCK' else 0
                fullEntry['is_upload_failed'] = 1 if fullEntry['state'] \
                    == 'UPLOAD-FAILED' else 0
                fullEntry['is_upload_successful'] = 1 if fullEntry['state'] \
                    == 'UPLOAD-SUCCESSFUL' else 0
            except KeyError:        # may be the case if <state> isn't passed through
                pass
            try:
                self._index(
                    index=index, documentID=fullEntry['rule_id'], body=fullEntry)
            except Exception as e:
                self.logger.warning("Failed to push rule: {}".format(e))
                continue

    def updateRuleWithDID(self, ruleID, index, extraEntries={}):
        """
        Update documents in the database corresponding to a rule with a given
        DID <ruleID> in index <index>. <extraEntries> key/value pairs will
        be appended to all entries prior to submission.
        """
        rucio = RucioWrappersAPI()
        try:
            self.logger.debug("Getting rule information...")
            rule = rucio.ruleInfo(ruleID)
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

        # The <fullEntry> pushed to the database is the concatenation of the two
        # dictionaries, <entry>, generated as above, and 'extraEntries', described
        # in the function definition.
        #
        fullEntry = {**entry, **extraEntries}

        # Add "throughput" field corresponding to time taken to change from
        # REPLICATING -> OK state. Note that this is a very crude throughput
        # estimation as this time is convolved with the polling frequency
        # of the rucio rules evalulator.
        #
        if fullEntry['state'] == 'OK':
            rtn = self._get(index, ruleID)
            if rtn['_source']['state'] == 'REPLICATING':
                doneAt = fullEntry['updated_at']
                startedReplicationAt = datetime.strptime(
                    rtn['_source']['updated_at'], "%Y-%m-%dT%H:%M:%S")
                fullEntry['replication_duration'] = (
                    doneAt-startedReplicationAt).total_seconds()

        # Add boolean flags for state. This makes it easy to use bucket
        # aggregations in ES queries.
        #
        fullEntry['is_done'] = 1 if fullEntry['state'] == 'OK' else 0
        fullEntry['is_replicating'] = 1 if fullEntry['state'] == 'REPLICATING' else 0
        fullEntry['is_stuck'] = 1 if fullEntry['state'] == 'STUCK' else 0

        # Append protocol and endpoint details to entry using result from
        # a separate call to list file replicas for this particular DID.
        #
        did = "{}:{}".format(fullEntry['scope'], fullEntry['name'])
        try:
            replica = rucio.listFileReplicas(did, rse=fullEntry['to_rse'])
            protocol = replica[0]['rses'][fullEntry['to_rse']][0].split(':')[0]
            endpoint = replica[0]['rses'][fullEntry['to_rse']][0]
        except Exception:    # if the rule doesn't exist
            endpoint = None
            protocol = None
        fullEntry['endpoint'] = endpoint
        fullEntry['protocol'] = protocol

        self.logger.info("Updating rule ({})...".format(ruleID))
        self._update(index=index, documentID=ruleID, body={"doc": fullEntry})
        self.logger.info("Update complete")
