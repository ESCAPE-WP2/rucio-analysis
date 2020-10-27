import requests

from db import ES
from tasks import Task


class AlarmStuck(Task):
    """ Send alarms about rule statuses. """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            databaseType = kwargs['database']['type']
            databaseUri = kwargs['database']['uri']
            databaseIndex = kwargs['database']['index'] 
            databaseSearchRangeLTE = kwargs['database']['search_range_lte']
            databaseSearchRangeGTE = kwargs['database']['search_range_gte']
            databaseMaxRows = kwargs['database']['max_rows']
            webhooks = kwargs['webhooks']
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            exit()

        if databaseType == 'es':
            es = ES(databaseUri, self.logger)

            # Query ES database for documents that are in a "STUCK" state
            #
            res = es.search(index=databaseIndex, maxRows=databaseMaxRows, body={
                "query": {
                    "bool": {
                        "filter": [{
                            "term": {
                                "is_stuck": 1
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
            nStuckDocs = len(res['hits']['hits'])

            # For each of these documents, add information about the alarm
            #
            attachments = []
            for idx, hit in enumerate(res['hits']['hits']):
                self.logger.info("Processing document {} of {}".format(
                    idx+1, nStuckDocs))

                ruleID = hit['_source']['rule_id']
                did = "{}:{}".format(hit['_source']['scope'], hit['_source']['name'])
                fromRSE = hit['_source']['from_rse']
                toRSE = hit['_source']['to_rse']
                createdAt = hit['_source']['created_at']
                #updatedAt = hit['_source']['updated_at']
                #expiresAt = hit['_source']['expires_at']
                try:
                    error = hit['_source']['error']
                except KeyError:
                    error = None

                for webhook in webhooks:
                    if webhook['type'] == 'slack':
                        attachments.append({
                            "fallback": "STUCK: {} to {} @ {}".format(
                                fromRSE, toRSE, createdAt),
                            "pretext": "STUCK: {} to {} @ {}".format(
                                fromRSE, toRSE, createdAt),
                            "color": "#D00000",
                            "fields": [
                                {
                                    "title": "Description",
                                    "value": "rule id:    {}\n".format(ruleID) +
                                             "did:        {}\n".format(did) +
                                             "error:      {}\n".format(error),
                                    "short": False
                                }
                            ]
                        })

            # For each webhook, process the alarm
            #
            for webhook in webhooks:
                if webhook['type'] == 'slack':
                    headers = {
                        'content-type': 'application/json',
                        'Accept-Charset': 'UTF-8'}

                    # Send detailed information about STUCK docs
                    #
                    body = {'attachments': attachments}
                    r = requests.post(webhook['url'], json=body, headers=headers)
        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
