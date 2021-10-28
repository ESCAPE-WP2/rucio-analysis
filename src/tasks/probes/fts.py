import datetime
import dateparser
import json
from kubernetes import client, config
import numpy as np
import requests
import pytz
import uuid

import fts3.rest.client.easy as fts3

from common.es.wrappers import Wrappers as ESWrappers
from tasks.task import Task


class ProbesFTSTransfers(Task):
    """ Get FTS transfer logs. """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()

        try:
            endpoint = kwargs["endpoint"]
            vo = kwargs["vo"]
            databases = kwargs["databases"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        transfers = dict(json.loads(requests.get(
            "{}/fts3/ftsmon/transfers?dest_se=&source_se=&time_window=1&vo={}".format(
                endpoint.rstrip('/'), vo), verify=False).content))['items']

        for transfer in transfers:
            if transfer['file_state'] == 'SUBMITTED':
                transfer['is_status_submitted'] = 1
            elif transfer['file_state'] == 'ACTIVE':
                transfer['is_status_active'] = 1
            elif transfer['file_state'] == 'FINISHED':
                transfer['is_status_finished'] = 1
            elif transfer['file_state'] == 'FAILED':
                transfer['is_status_failed'] = 1
            else:
                transfer['is_status_other'] = 1

        # Push corresponding logs to database
        if databases is not None:
            for database in databases:
                if database["type"] == "es":
                    self.logger.debug("Injecting information into ES database...")
                    es = ESWrappers(database["uri"], self.logger)
                    for transfer in transfers:
                        es._index(
                            index=database["index"],
                            documentID=transfer['file_id'],
                            body={
                                '@timestamp': int(datetime.datetime.now().strftime("%s"))*1000,
                                **transfer
                            }
                        )

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
