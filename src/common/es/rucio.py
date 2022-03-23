from datetime import datetime
import json
import numpy as np
import uuid

from common.es.wrappers import Wrappers
from common.rucio.wrappers import RucioWrappersAPI


class Rucio(Wrappers):
    """
        Additional functionality for pushing Rucio related information to an
        ElasticSearch index.
    """

    def pushRulesForDID(self, did, index, baseEntry={}):
        """
        Create documents in the database corresponding to replication rules for
        a given DID <did> and add to index <index>. <baseEntry> key/value
        pairs will be appended to all entries prior to submission.
        """
        rucio = RucioWrappersAPI()
        rules = rucio.listReplicationRules(did) if did is not None else []
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

                # FTS attribute placeholders
                entry['fts_throughput'] = None

                entries.append(entry)
        else:   # otherwise, create entry with timestamp and "spoofed" rule_id
            now = datetime.now()
            entries.append({
                '@timestamp': int(now.strftime("%s"))*1000,
                'rule_id': str(uuid.uuid4())
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
            fullEntry['is_done'] = 1 if fullEntry.get('state') == 'OK' else 0
            fullEntry['is_replicating'] = 1 if fullEntry.get('state') \
                == 'REPLICATING' else 0
            fullEntry['is_stuck'] = 1 if fullEntry.get('state') \
                == 'STUCK' else 0
            fullEntry['is_upload_failed'] = 1 if fullEntry.get('state') \
                == 'UPLOAD-FAILED' else 0
            fullEntry['is_upload_successful'] = 1 if fullEntry.get('state') \
                == 'UPLOAD-SUCCESSFUL' else 0
            try:
                self._index(
                    index=index, documentID=fullEntry['rule_id'], body=fullEntry)
            except Exception as e:
                self.logger.warning("Failed to push rule: {}".format(e))
                continue

    def updateRuleWithDID(self, ruleID, index, ftsEndpoint, extraEntries={}):
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

        # Add field corresponding to time taken to change from
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

        # If fts endpoint is specified and the state is now OK, add throughput field using fts job query.
        #
        if fullEntry['state'] == 'OK':
            if ftsEndpoint:
                try:
                    self.logger.info("Attempting to get throughput from FTS...")

                    import fts3.rest.client.easy as fts3

                    DIDs = rucio.listContent(scope=entry['scope'], name=entry['name'])
                    if DIDs:    # container, so recurse into it.
                        while True:
                            startIterationNumberOfDIDs = len(DIDs)
                            for DID in DIDs:
                                recursedDIDs = rucio.listContent(scope=DID['scope'], name=DID['name'])
                                if recursedDIDs:
                                    DIDs = DIDs + recursedDIDs
                            if startIterationNumberOfDIDs == len(DIDs):     # i.e. all DIDs are files.
                                break
                    else:
                        DIDs = [{
                            'scope': entry['scope'],
                            'name': entry['name']
                        }]

                    # Get throughput (or average if DID is container).
                    #
                    throughputs = []
                    for DID in DIDs:
                        try:
                            # Get request from historical requests table.
                            #
                            requests = rucio.getRequestHistory(did='{}:{}'.format(DID['scope'], 
                                DID['name']), rse=entry['to_rse'])
                            external_id = requests['external_id']
    
                            # Get the fts job_id (external_id) from the above request, and query
                            # fts for information about this job.
                            #
                            context = fts3.Context(ftsEndpoint, verify=False)
                            files = json.loads(context.get("/jobs/" + external_id + '/files'))

                            throughputs = throughputs + [fi["throughput"] for fi in files]
                        except Exception as e:
                            self.logger.warning("Error getting throughput: {}".format(e))
                    if throughputs:
                        fullEntry['fts_throughput_mean'] = np.mean(throughputs)
                        fullEntry['fts_throughput_median'] = np.median(throughputs)
                        fullEntry['fts_throughput_stdev'] = np.std(throughputs)
                        self.logger.debug("Added throughput ({}/{}/{}).".format(
                            fullEntry['fts_throughput_mean'],
                            fullEntry['fts_throughput_median'],
                            fullEntry['fts_throughput_stdev']
                        ))
                except Exception as e:
                    self.logger.warning("Failed to update throughput from FTS: {}".format(e))

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

    def _update(self, index, documentID, body):
        """ Update an existing document with id, <documentID>, in index, <index>. """
        try:
            self.es.update(index=index, id=documentID, body=body)
        except Exception as e:
            self.logger.warning("Failed to update database: {}".format(e))

    def search(self, index, body, maxRows=10000):
        return self._search(index, body, maxRows)
