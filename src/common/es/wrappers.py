from elasticsearch import Elasticsearch


class Wrappers():
    """
    Common functionality for interaction with ElasticSearch backends.
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
            res = self.es.index(index=index, id=documentID, body=body)
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
