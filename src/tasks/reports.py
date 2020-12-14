from datetime import datetime
import requests

from slack import RTMClient

from db import ES
from tasks import Task


class ReportDaily(Task):
    """ Reports. """

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
            percentageStuckWarningThreshold = kwargs[
                'percentage_stuck_warning_threshold']
            rses = kwargs['rses']
            webhooks = kwargs['webhooks']
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        if databaseType == 'es':
            es = ES(databaseUri, self.logger)

            blocks = []
            blocks.append({
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "Daily Report ({})".format(
                        datetime.now().strftime("%d-%m-%Y"))
                }
            })
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "Stuck Threshold (%): {}".format(
                        percentageStuckWarningThreshold)
                },
            })
            blocks.append({
                "type": "divider"
            })
            for rse in rses:
                nDocsAsSrc = {}
                nDocsAsDst = {}
                for term in ['is_submitted', 'is_stuck', 'is_replicating', 'is_done']:
                    res = es.search(index=databaseIndex, maxRows=databaseMaxRows,
                    body={
                        "query": {
                            "bool": {
                                "filter": [{
                                    "term": {
                                        term: 1
                                    }
                                }, {
                                    "term": {
                                        'task_name.keyword': 'test-replication'
                                    }
                                }, {
                                    "term": {
                                        "from_rse.keyword": rse
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
                    nDocsAsSrc[term] = len(res['hits']['hits'])

                    res = es.search(index=databaseIndex, maxRows=databaseMaxRows,
                    body={
                        "query": {
                            "bool": {
                                "filter": [{
                                    "term": {
                                        term: 1
                                    }
                                }, {
                                    "term": {
                                        'task_name.keyword': 'test-replication'
                                    }
                                }, {
                                    "term": {
                                        "to_rse.keyword": rse
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
                    nDocsAsDst[term] = len(res['hits']['hits'])

                for webhook in webhooks:
                    if webhook['type'] == 'slack':
                        url = 'https://monit-grafana.cern.ch/d/O8MinE5Gk/es-ska-rmb?' +\
                        'orgId=51&from={}&to={}&var-rses={}'.format(
                            databaseSearchRangeGTE, 
                            databaseSearchRangeLTE, 
                            rse)

                        blocks.append({
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": "*<{}|{}>*".format(url, rse)
                            }
                        })

                        if nDocsAsSrc['is_stuck'] > percentageStuckWarningThreshold or \
                        nDocsAsSrc['is_submitted'] == 0:
                            symbol = ':warning:'
                        else:
                            symbol = ':ok:'
                        blocks.append({
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": "{}\tAs Source\t\t\t:arrow_up: {:4d}\t:".format(
                                        symbol, nDocsAsSrc['is_submitted']) +
                                    "heavy_check_mark: {:4d}\t".format(
                                        nDocsAsSrc['is_done']) +
                                    ":x: {:4d}\t:".format(
                                        nDocsAsSrc['is_stuck']) +
                                    "arrow_forward: {:4d}".format(
                                        nDocsAsSrc['is_replicating'])
                            }
                        })

                        if nDocsAsDst['is_stuck'] > percentageStuckWarningThreshold or \
                        nDocsAsDst['is_submitted'] == 0:
                            symbol = ':warning:'
                        else:
                            symbol = ':ok:'
                        blocks.append({
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": "{}\tAs Destination\t:arrow_up: {:4d}\t:".format(
                                        symbol, nDocsAsDst['is_submitted']) +
                                    "heavy_check_mark: {:4d}\t".format(
                                        nDocsAsDst['is_done']) +
                                    ":x: {:4d}\t:".format(
                                        nDocsAsDst['is_stuck']) +
                                    "arrow_forward: {:4d}".format(
                                        nDocsAsDst['is_replicating'])
                            }
                        })
                        blocks.append({
                            "type": "divider"
                        })

            # For each webhook, process the report
            #
            for webhook in webhooks:
                if webhook['type'] == 'slack':
                    headers = {
                        'content-type': 'application/json',
                        'Accept-Charset': 'UTF-8'}

                    # Send report information
                    #
                    body = {'blocks': blocks}
                    r = requests.post(webhook['url'], json=body, headers=headers)
        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
