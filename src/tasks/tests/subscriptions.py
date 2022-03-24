import os

from common.rucio.admin_wrappers import RucioAdminWrappersAPI
from common.rucio.helpers import createCollection
from common.rucio.wrappers import RucioWrappersAPI
from tasks.task import Task


class TestCreateSubscription(Task):
    """ Create empty dataset and subscription. """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            # Assign variables from kwargs.
            #
            scope = kwargs["scope"]
            fixedMetadata = kwargs["fixed_metadata"]
            datasetName = kwargs["dataset_name"]
            subscriptionName = kwargs["subscription_name"]
            filter = kwargs["filter"]
            replicationRules = kwargs["replication_rules"]
            comments = kwargs["comments"]
            subscriptionLifetime = kwargs["subscription_lifetime"]
            retroactive = kwargs["retroactive"]
            dryRun = kwargs["dry_run"]
            priority = kwargs["priority"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        # Instantiate RucioWrappers class to allow access to static methods.
        #
        rucio = RucioWrappersAPI()
        rucioAdmin = RucioAdminWrappersAPI()

        # Create a dataset to house the data, named with today's date
        # and scope <scope>.
        #
        self.logger.info("Creating a new dataset")
        datasetDID = createCollection(self.logger.name, scope, name=datasetName)

        # Set metadata on dataset
        #
        try:
            self.logger.info("Setting metadata on dataset {}".format(datasetDID))
            rucio.setMetadataBulk(datasetDID, fixedMetadata)
        except Exception as e:
            self.logger.warning(repr(e))
            self.logger.info(
                "Bulk metadata call may have failed, trying to set metadata fields individually."
            )
            for key, value in fixedMetadata.items():
                rucio.setMetadata(datasetDID, key, value)

        # Create metadata-based subscription
        #
        self.logger.info("Creating/updating subscription {}".format(subscriptionName))
        existingSubs = rucioAdmin.listSubscriptions()
        if subscriptionName in (sub["name"] for sub in existingSubs):
            self.logger.info(
                "Subscription {} already exists, will update".format(subscriptionName)
            )
            rucioAdmin.updateSubscription(
                subscriptionName,
                account=os.environ["RUCIO_CFG_ACCOUNT"],
                filter=filter,
                replication_rules=replicationRules,
                comments=comments,
                lifetime=subscriptionLifetime,
                retroactive=retroactive,
                dry_run=dryRun,
                priority=priority,
            )
        else:
            self.logger.info("Creating subscription {}".format(subscriptionName))
            rucioAdmin.addSubscription(
                subscriptionName,
                account=os.environ["RUCIO_CFG_ACCOUNT"],
                filter=filter,
                replication_rules=replicationRules,
                comments=comments,
                lifetime=subscriptionLifetime,
                retroactive=retroactive,
                dry_run=dryRun,
                priority=priority,
            )
