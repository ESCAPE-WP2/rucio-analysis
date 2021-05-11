from datetime import datetime
import logging

from common.rucio.wrappers import RucioWrappersAPI


def createCollection(loggerName, scope, name=None, collectionType="DATASET"):
    """ Create a new collection in scope, <scope>. """

    logger = logging.getLogger(loggerName)

    rucio = RucioWrappersAPI()

    # If name is not specified, create one according to datestamp.
    #
    if name is None:
        name = datetime.now().strftime("%d-%m-%Y")
        if collectionType == "CONTAINER":
            name = "container_{}".format(name)

    # Create container DID according to <scope>:<name> format.
    #
    did = "{}:{}".format(scope, name)

    logger.info("Checking to see if DID ({}) already exists...".format(did))
    try:
        # Check to see if DID already exists, and if not, add.
        dids = rucio.listDIDs(scope=scope)
        if did not in dids:
            logger.debug("Adding DID {} of type {}".format(did, collectionType))
            try:
                rucio.addDID(did, collectionType)
            except Exception as e:
                logger.critical("Error adding did.")
                logger.critical(repr(e))
                return False
        else:
            logger.debug("DID already exists. Skipping.")

    except Exception as e:
        logger.critical("Error listing collection.")
        logger.critical(repr(e))
        return False

    return did
