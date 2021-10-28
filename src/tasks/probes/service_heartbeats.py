import datetime
import dateparser
import json
import requests
import uuid

from common.es.wrappers import Wrappers as ESWrappers
from tasks.task import Task


class ProbesServiceHeartbeats(Task):
    """ Get heartbeats for services. """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()

        try:
            services = kwargs["services"]
            databases = kwargs["databases"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        for svc in services:
            response = requests.get(svc['endpoint'], verify=False)
            if response.status_code == svc['expected_status_code']:
                if svc['expected_content']:
                    if dict(json.loads(response.content)) == svc['expected_content']:
                        svc['is_alive'] = 1
                        svc['error'] = None
                    else:
                        svc['is_alive'] = 0
                        svc['error'] = 'content mismatch, expected {} got {}'.format(
                            repr(svc['expected_content']), repr(dict(json.loads(response.content)))
                        )
                else:
                    svc['is_alive'] = 1
                    svc['error'] = None
            else:
                svc['is_alive'] = 0
                svc['error'] = 'status code mismatch, expected {} got {}'.format(
                    svc['expected_status_code'], response.status_code
                )

        # Push corresponding logs to database
        if databases is not None:
            for database in databases:
                if database["type"] == "es":
                    self.logger.debug("Injecting information into ES database...")
                    es = ESWrappers(database["uri"], self.logger)
                    for svc in services:
                        es._index(
                            index=database["index"],
                            documentID=svc['name'],
                            body={
                                '@timestamp': int(datetime.datetime.now().strftime("%s"))*1000,
                                'service_name': svc['name'],
                                'service_endpoint': svc['endpoint'],
                                'is_alive': svc['is_alive'],
                                'error': svc['error'],
                            }
                        )

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
