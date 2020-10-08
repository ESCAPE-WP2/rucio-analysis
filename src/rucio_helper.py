from rucio_wrappers import RucioWrappersAPI, RucioWrappersCLI
from datetime import datetime

QOS_POLICIES = ["SAFE", "FAST", "CHEAP-ANALYSIS", "OPPORTUNISTIC"]


def create_did(
    logger,
    scope,
):
    # Instantiate RucioWrappers class to allow access to static methods.
    #
    rucio = RucioWrappersCLI()

    # Create a dataset to house the data, named with today's date
    # and scope <scope>.
    #
    # Does not try to create a dataset if it already exists.
    #
    todaysDate = datetime.now().strftime("%d-%m-%Y")
    datasetDID = "{}:{}".format(scope, todaysDate)
    logger.info("Checking for dataset ({})".format(datasetDID))
    try:
        dids = rucio.listDIDs(scope=scope)
    except Exception as e:
        logger.critical("Error listing dataset")
        logger.critical(repr(e))
        exit()
    if datasetDID not in dids:
        logger.debug("Adding dataset")
        try:
            rucio.addDataset(did=datasetDID)
        except Exception as e:
            logger.critical("Error adding dataset")
            logger.critical(repr(e))
            exit()
    else:
        logger.debug("Dataset already exists")

    return datasetDID


def list_rse_qos(rses):
    """ Return QoS for list of RSEs """

    rucio = RucioWrappersAPI()

    rse_qos = {}
    for rse in rses:
        qos = rucio.list_rse_attributes(rse)["QOS"]
        if qos not in QOS_POLICIES:
            print(qos)
            raise Exception()
        rse_qos[rse] = qos
    return rse_qos
