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


class ProbesServiceMetricsFTS(Task):
    """ Get metrics for FTS. """

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

        metrics = {}

        transfers = dict(json.loads(requests.get(
            "{}/fts3/ftsmon/transfers?dest_se=&source_se=&time_window=1&vo={}".format(
                endpoint.rstrip('/'), vo), verify=False).content))['items']
        jobs = dict(json.loads(requests.get(
            "{}/fts3/ftsmon/jobs?dest_se=&source_se=&time_window=1&vo={}".format(
                endpoint.rstrip('/'), vo), verify=False).content))['items']

        # get delays between transfer start and job submission
        delays = []
        for transfer in transfers:
            try:
                job = next(job for job in jobs if job["job_id"] == transfer['job_id'])
            except StopIteration:
                continue
            jobSubmitTime = dateparser.parse(job['submit_time'])
            transferStartTime = dateparser.parse(transfer['start_time'])

            delays.append((transferStartTime-jobSubmitTime).total_seconds())

        metrics['average_delay'] = {
            'name': 'Average queuing delay (s)',
            'value': np.mean(delays)
        }

        # get the time since the last job submission
        last = jobs[0]
        metrics['last_job_submission'] = {
            'name': 'Seconds since last job submission',
            'value': (datetime.datetime.utcnow().replace(tzinfo=pytz.UTC) - \
                dateparser.parse(last['submit_time'])).total_seconds()
        }

        # get the time since the last successful transfer
        lastSuccess = next(transfer for transfer in transfers if transfer['file_state'] == 'FINISHED')
        metrics['last_successful_transfer'] = {
            'name': 'Seconds since last successful transfer',
            'value': (datetime.datetime.utcnow().replace(tzinfo=pytz.UTC) - \
                dateparser.parse(lastSuccess['start_time'])).total_seconds()
        }

        # Push corresponding logs to database
        if databases is not None:
            for database in databases:
                if database["type"] == "es":
                    self.logger.debug("Injecting information into ES database...")
                    es = ESWrappers(database["uri"], self.logger)
                    for key, metric in metrics.items():
                        es._index(
                            index=database["index"],
                            documentID=key,
                            body={
                                '@timestamp': int(datetime.datetime.now().strftime("%s"))*1000,
                                'service_name': "fts",
                                'metric_name': metric['name'],
                                'metric_value': metric['value']
                            }
                        )

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
