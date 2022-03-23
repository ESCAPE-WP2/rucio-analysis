from datetime import datetime
import requests

from common.es.rucio import Rucio as ESRucio
from tasks.task import Task


class ReportDaily(Task):
    """ Generate a daily report and post to a webhook. """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()

        try:
            databaseType = kwargs["database"]["type"]
            databaseUri = kwargs["database"]["uri"]
            databaseIndex = kwargs["database"]["index"]
            databaseSearchRangeLTE = kwargs["database"]["search_range_lte"]
            databaseSearchRangeGTE = kwargs["database"]["search_range_gte"]
            databaseMaxRows = kwargs["database"]["max_rows"]
            percentageStuckWarningThreshold = kwargs[
                "percentage_stuck_warning_threshold"
            ]
            reportTitle = kwargs["report_title"]
            rses = kwargs["rses"]
            usingTaskName = kwargs["using_task_name"]
            webhooks = kwargs["webhooks"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        # Retrieve data for the report from the database.
        #
        if databaseType == "es":
            es = ESRucio(databaseUri, self.logger)

            nDocsAsSrc = {}
            nDocsAsDst = {}
            for rse in rses:
                nDocsAsSrc[rse] = {}
                nDocsAsDst[rse] = {}
                # Query the database for the number of documents in each
                # state.
                #
                for term in [
                    "is_submitted",
                    "is_stuck",
                    "is_replicating",
                        "is_done"]:
                    # With RSE as src.
                    res = es.search(
                        index=databaseIndex,
                        maxRows=databaseMaxRows,
                        body={
                            "query": {
                                "bool": {
                                    "filter": [
                                        {"term": {term: 1}},
                                        {
                                            "term": {
                                                "task_name.keyword":
                                                    usingTaskName
                                            }
                                        },
                                        {"term": {"from_rse.keyword": rse}},
                                        {
                                            "range": {
                                                "created_at": {
                                                    "gte":
                                                        databaseSearchRangeGTE,
                                                    "lte":
                                                        databaseSearchRangeLTE,
                                                }
                                            }
                                        },
                                    ]
                                }
                            }
                        },
                    )
                    nDocsAsSrc[rse][term] = len(res["hits"]["hits"])

                    # With RSE as dst.
                    res = es.search(
                        index=databaseIndex,
                        maxRows=databaseMaxRows,
                        body={
                            "query": {
                                "bool": {
                                    "filter": [
                                        {"term": {term: 1}},
                                        {"term": {
                                                "task_name.keyword":
                                                    usingTaskName
                                            }
                                        },
                                        {"term": {"to_rse.keyword": rse}},
                                        {
                                            "range": {
                                                "created_at": {
                                                    "gte":
                                                        databaseSearchRangeGTE,
                                                    "lte":
                                                        databaseSearchRangeLTE,
                                                }
                                            }
                                        },
                                    ]
                                }
                            }
                        },
                    )
                    nDocsAsDst[rse][term] = len(res["hits"]["hits"])
        
        # Format the report depending on the webhook type.
        #
        for webhook in webhooks:
            if webhook["type"] == "slack":
                blocks = []

                # Header.
                #
                blocks.append({
                    "type": "header",
                    "text": {
                            "type": "plain_text",
                            "text": "{} ({})".format(reportTitle, 
                                datetime.now().strftime("%d-%m-%Y")
                            ),
                    },
                })
                blocks.append({
                    "type": "section",
                    "text": {
                            "type": "mrkdwn",
                            "text": "Stuck Threshold (%): {}".format(
                                percentageStuckWarningThreshold
                            ),
                    },
                })
                blocks.append({"type": "divider"})

                # Add blocks for number of documents in each state.
                #
                for rse in rses:
                    blocks.append({
                        "type": "section",
                        "text": {
                                "type": "mrkdwn",
                                "text": "*{}*".format(rse),
                        },
                    })
                    if nDocsAsSrc[rse]["is_stuck"] > percentageStuckWarningThreshold \
                            or nDocsAsSrc[rse]["is_submitted"] == 0:
                        symbol = ":warning:"
                    else:
                        symbol = ":ok:"
                    blocks.append({
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text":
                                "{}\tAs src\t:arrow_up: {:4d}\t:".format(
                                    symbol, nDocsAsSrc[rse]["is_submitted"]
                                )
                            + "heavy_check_mark: {:4d}\t".format(
                                nDocsAsSrc[rse]["is_done"]
                                    )
                            + ":x: {:4d}\t:".format(nDocsAsSrc[rse]["is_stuck"])
                            + "arrow_forward: {:4d}".format(
                                nDocsAsSrc[rse]["is_replicating"]
                                    )
                        },
                    })
                    if nDocsAsDst[rse]["is_stuck"] > percentageStuckWarningThreshold \
                            or nDocsAsDst[rse]["is_submitted"] == 0:
                        symbol = ":warning:"
                    else:
                        symbol = ":ok:"
                    blocks.append({
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text":
                                "{}\tAs dst\t:arrow_up: {:4d}\t:".format(
                                    symbol, nDocsAsDst[rse]["is_submitted"]
                                ) + "heavy_check_mark: {:4d}\t".format(
                                    nDocsAsDst[rse]["is_done"])
                            + ":x: {:4d}\t:".format(nDocsAsDst[rse]["is_stuck"])
                            + "arrow_forward: {:4d}".format(
                                    nDocsAsDst[rse]["is_replicating"]),
                        },
                    })
                    blocks.append({"type": "divider"})

                # Add payload headers.
                #
                headers = {
                    "content-type": "application/json",
                    "Accept-Charset": "UTF-8",
                }

                # Send report information.
                #
                body = {"blocks": blocks}
                requests.post(webhook["url"], json=body, headers=headers)
        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
