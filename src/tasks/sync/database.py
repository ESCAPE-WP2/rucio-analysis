import logging
from multiprocessing import Pool

from common.es.rucio import Rucio as ESRucio
from tasks.task import Task


class SyncESDatabase(Task):
    """ Synchronise ES database with Rucio rule statuses. """

    def __init__(self, logger):
        super().__init__(logger)

    @staticmethod
    def _async_updateRuleWithDID(loggerName, idx, databaseUri, ruleID, databaseIndex, ftsEndpoint):
        logger = logging.getLogger(loggerName)
        logger.info("Processing entry #{}".format(idx))
        es = ESRucio(databaseUri, logger)
        es.updateRuleWithDID(ruleID, databaseIndex, ftsEndpoint)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            ftsEndpoint = kwargs['fts_endpoint']
            taskNameToUpdate = kwargs['task_name_to_update']
            nWorkers = kwargs['n_workers']
            updateAll = kwargs['update_all']
            databaseUri = kwargs['database']['uri']
            databaseIndex = kwargs['database']['index']
            databaseSearchRangeLTE = kwargs['database']['search_range_lte']
            databaseSearchRangeGTE = kwargs['database']['search_range_gte']
            databaseMaxRows = kwargs['database']['max_rows']
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        es = ESRucio(databaseUri, self.logger)

        # Query ES database for documents.
        #
        self.logger.info("Querying database for documents to be updated...")
        query = {
            "query": {
                "bool": {
                    "filter": [{
                        "term": {
                            "task_name.keyword": taskNameToUpdate
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
        }
        if not updateAll:   # filter for only 'not done' submissions
            query['query']['bool']['filter'].append(
                {
                    "term": {
                        "is_done": 0
                    }
                }
            )
            
        res = es.search(index=databaseIndex, maxRows=databaseMaxRows, body=query)
        nDocs = len(res['hits']['hits'])
        self.logger.info("Found {} documents".format(nDocs))

        # For each of these documents, try to update fields in the ES database.
        #
        pool = Pool(nWorkers)
        for idx, hit in enumerate(res['hits']['hits']):
            ruleID = hit['_source']['rule_id']
            pool.apply_async(self._async_updateRuleWithDID, args=(
                self.logger.name, idx, databaseUri, ruleID, databaseIndex, ftsEndpoint))
        pool.close()
        pool.join()

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
