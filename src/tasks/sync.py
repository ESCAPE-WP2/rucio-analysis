import logging
from multiprocessing import Pool

from db import ES
from tasks import Task


class SyncDatabase(Task):
    """ Update ES database rules. """

    def __init__(self, logger):
        super().__init__(logger)

    @staticmethod
    def _updateDBAsync(loggerName, databaseUri, ruleID, databaseIndex):
        logger = logging.getLogger(loggerName)
        es = ES(databaseUri, logger)
        es.updateRuleWithDID(ruleID, databaseIndex)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            taskNameToUpdate = kwargs['task_name_to_update']
            nWorkers = kwargs['n_workers']
            databaseType = kwargs['database']['type']
            databaseUri = kwargs['database']['uri']
            databaseIndex = kwargs['database']['index'] 
            databaseSearchRangeLTE = kwargs['database']['search_range_lte']
            databaseSearchRangeGTE = kwargs['database']['search_range_gte']
            databaseMaxRows = kwargs['database']['max_rows']
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            exit()

        if databaseType == 'es':
            es = ES(databaseUri, self.logger)

            # Query ES database for documents that are not in an "OK" state
            #
            self.logger.info("Querying database for documents to be updated...")
            res = es.search(index=databaseIndex, maxRows=databaseMaxRows, body={
                "query": {
                    "bool": {
                        "filter": [{
                            "term": {
                                'task_name.keyword': taskNameToUpdate
                            }
                        }, {
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
            nDocs = len(res['hits']['hits'])
            self.logger.info("Found {} documents".format(nDocs))

            # For each of these documents, try to update fields in the ES database.
            #
            pool = Pool(nWorkers)
            for idx, hit in enumerate(res['hits']['hits']):
                ruleID = hit['_source']['rule_id']
                pool.apply_async(self._updateDBAsync, args=(
                    self.logger.name, databaseUri, ruleID, databaseIndex))
            pool.close()
            pool.join()

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
